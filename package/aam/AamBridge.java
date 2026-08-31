import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * YwMatrix ↔ 工行 AAM（aam-sm-2.0.jar，com.icbc.ssic.*）桥 —— stdio 单请求协议。
 *
 * 用法（一次性进程：一次登录验签起一次，无需进程管理）：
 *   java -cp .:/path/to/aam-sm-2.0.jar:/path/to/servlet-api.jar AamBridge
 *   （若 SDK jar 自带 servlet 类，servlet-api 可省；classpath 用目录通配 aam/* 即全部收入）
 *
 * stdin 一行 JSON（配置 + 本次参数，ssiAuth/ssiSign 可为空串）：
 *   {"serverName":"aam.icbc","version":"SM2","serviceName":"YourApp",
 *    "serviceURL":"https://gw.example/auth/aam/callback",
 *    "smPublicKey":"{...}","smKeyPass":"...",
 *    "ssiAuth":"...","ssiSign":"..."}
 * stdout 一行 JSON，两种结果：
 *   验签成功：{"ok":true,"employeeNo":"...","username":"...","name":"...","department":"..."}
 *   其余：   {"ok":false,"error":"redirect|原因","redirect":"SDK 要求跳转的地址"}
 *
 * 两种调用形态（对应 servlet 过滤器的同一入口）：
 * - 登录入口（ssiAuth/ssiSign 传空串）：SDK 内部 sendRedirect 到 AAM 登录页，
 *   桥把该地址放在 redirect 字段返回，网关拿去 302 浏览器
 * - 回调验签（带 ssiAuth/ssiSign）：验签成功取工号；失败 SDK 也可能
 *   redirect（如会话失效重回登录页），同样经 redirect 字段透传
 *
 * 实现要点（对接文档 3.1/3.2 节的标准用法）：
 * - ServerSideAuthenticator 六个 setter 初始化 → setOperation("signIn")
 *   → execute(req, resp, ssiAuth, ssiSign) 返回 boolean
 * - execute 需要 HttpServletRequest/Response：用动态代理造假对象——
 *   属性表真实实现（SDK 把 Credentials 放进 request attribute）、
 *   sendRedirect 被捕获、getWriter 给哑 Writer；其余方法打 stderr 日志后返回类型默认值，
 *   首次实测看日志即知 SDK 还依赖什么
 * - ssiAuth/ssiSign 先 replaceAll("[\\r\\n]","")（文档明确：不去会验签失败）
 * - 全部反射调用，编译时不需要 SDK jar；类名可用 -Daam.auth.class= 覆盖
 */
public class AamBridge {
    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        PrintWriter out = new PrintWriter(System.out, true);
        String line = in.readLine();
        if (line == null || line.trim().isEmpty()) {
            out.println(json(false, "empty input", null));
            return;
        }
        try {
            out.println(verify(line.trim()));
        } catch (Throwable e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            out.println(json(false, String.valueOf(cause), null));
        }
    }

    private static String verify(String input) throws Exception {
        String serverName = field(input, "serverName");
        String version = orDefault(field(input, "version"), "SM2");
        String serviceName = field(input, "serviceName");
        String serviceURL = field(input, "serviceURL");
        String smPublicKey = field(input, "smPublicKey");
        String smKeyPass = field(input, "smKeyPass");
        // 文档要求：回跳参数先去掉 \r\n，否则签名校验失败。
        // 登录入口形态允许为空串（SDK 会 sendRedirect 到 AAM 登录页）
        String ssiAuth = stripCrlf(orEmpty(field(input, "ssiAuth")));
        String ssiSign = stripCrlf(orEmpty(field(input, "ssiSign")));
        if (serviceName == null || serviceURL == null || smPublicKey == null) {
            return json(false, "missing required fields (serviceName/serviceURL/smPublicKey)", null);
        }

        // ---- 初始化 ServerSideAuthenticator（文档 3.1）----
        String className = System.getProperty("aam.auth.class", "com.icbc.ssic.base.ServerSideAuthenticator");
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            return json(false, "class not found: " + className + "（检查 classpath 是否含 aam-sm-2.0.jar）", null);
        }
        Object auth = clazz.getDeclaredConstructor().newInstance();
        invokeSetter(auth, "setServerName", serverName);
        invokeSetter(auth, "setVersion", version);
        invokeSetter(auth, "setServiceName", serviceName);
        invokeSetter(auth, "setServiceURL", serviceURL);
        invokeSetter(auth, "setSmPublicKey", smPublicKey);
        invokeSetter(auth, "setSmKeyPass", smKeyPass);

        // ---- 假 servlet 对象（SDK 的 execute 签名需要）----
        Class<?> reqClass = servletClass("javax.servlet.http.HttpServletRequest", "jakarta.servlet.http.HttpServletRequest");
        Class<?> respClass = servletClass("javax.servlet.http.HttpServletResponse", "jakarta.servlet.http.HttpServletResponse");
        final Map<String, Object> attrs = new HashMap<>();
        final String[] redirect = { null };
        Object req = Proxy.newProxyInstance(AamBridge.class.getClassLoader(), new Class<?>[]{ reqClass },
                (proxy, m, a) -> {
                    String n = m.getName();
                    if (n.equals("getAttribute")) return attrs.get(String.valueOf(a[0]));
                    if (n.equals("setAttribute")) { attrs.put(String.valueOf(a[0]), a[1]); return null; }
                    if (n.equals("getParameter")) return null;
                    debug("request." + n + "() 被调用（返回默认值）");
                    return defaultValue(m.getReturnType());
                });
        ByteArrayOutputStream respBytes = new ByteArrayOutputStream();
        PrintWriter respWriter = new PrintWriter(respBytes, true);
        Object resp = Proxy.newProxyInstance(AamBridge.class.getClassLoader(), new Class<?>[]{ respClass },
                (proxy, m, a) -> {
                    String n = m.getName();
                    if (n.equals("sendRedirect")) { redirect[0] = String.valueOf(a[0]); return null; }
                    if (n.equals("getWriter")) return respWriter;
                    if (n.equals("setHeader") || n.equals("addHeader") || n.equals("addCookie")
                            || n.equals("setStatus") || n.equals("setContentType")) return null;
                    debug("response." + n + "() 被调用（返回默认值）");
                    return defaultValue(m.getReturnType());
                });

        // ---- 登录验签（文档 3.2）----
        clazz.getMethod("setOperation", String.class).invoke(auth, "signIn");
        Method execute = null;
        for (Method m : clazz.getMethods()) {
            if (m.getName().equals("execute") && m.getParameterCount() == 4) { execute = m; break; }
        }
        if (execute == null) return json(false, "SDK 无 execute(HttpServletRequest,HttpServletResponse,String,String) 方法", null);
        Object ok = execute.invoke(auth, req, resp, ssiAuth, ssiSign);
        // SDK 发出了 sendRedirect：登录入口的正常形态 / 验签失败重登，统一经 redirect 透传
        if (redirect[0] != null && !redirect[0].isEmpty()) {
            return json(false, "redirect", redirect[0]);
        }
        if (!(ok instanceof Boolean) || !(Boolean) ok) {
            return json(false, "验签失败（execute 返回 false 且无跳转）", null);
        }
        Object cred = attrs.get("ssiCredentials");
        if (cred == null) return json(false, "验签成功但未取到 ssiCredentials 属性", null);
        Object user = cred.getClass().getMethod("getSSICUser").invoke(cred);
        if (user == null) return json(false, "Credentials 中 SSICUser 为空", null);

        // SSICUserInfo 字段：employeeNo/username/name/department/fullName...（均有 getter）
        StringBuilder sb = new StringBuilder("{\"ok\":true");
        String[] pick = { "employeeNo", "username", "name", "fullName", "department" };
        for (String f : pick) {
            String v = callGetter(user, f);
            if (v != null && !v.isEmpty()) sb.append(",\"").append(f).append("\":\"").append(escape(v)).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    // ---- 反射小工具 ----

    static void invokeSetter(Object target, String setter, String value) throws Exception {
        try {
            target.getClass().getMethod(setter, String.class).invoke(target, value);
        } catch (NoSuchMethodException e) {
            debug(setter + " 不存在于 " + target.getClass().getName() + "，跳过");
        }
    }

    static String callGetter(Object target, String field) {
        String getter = "get" + Character.toUpperCase(field.charAt(0)) + field.substring(1);
        try {
            Object v = target.getClass().getMethod(getter).invoke(target);
            return v == null ? null : String.valueOf(v);
        } catch (Exception e) {
            return null;
        }
    }

    static Class<?> servletClass(String... names) throws Exception {
        for (String n : names) {
            try { return Class.forName(n); } catch (ClassNotFoundException ignore) { }
        }
        throw new IllegalStateException(
                "classpath 缺 servlet API（javax/jakarta.servlet.http.*）——把 servlet-api.jar 放进 aam/ 目录");
    }

    static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == char.class) return '\0';
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        return null;
    }

    static String stripCrlf(String s) {
        return s == null ? null : s.replaceAll("[\\r\\n]", "");
    }

    static String orDefault(String v, String d) { return v == null || v.isEmpty() ? d : v; }

    static String orEmpty(String v) { return v == null ? "" : v; }

    static void debug(String msg) {
        System.err.println("[aam-bridge] " + msg);
    }

    // ---- 极简 JSON 字段抽取（协议只有一层平铺字段，不引 JSON 依赖） ----

    static String field(String json, String key) {
        int i = json.indexOf("\"" + key + "\"");
        if (i < 0) return null;
        int colon = json.indexOf(':', i + key.length() + 2);
        if (colon < 0) return null;
        int q1 = json.indexOf('"', colon);
        if (q1 < 0) return null;
        StringBuilder sb = new StringBuilder();
        for (int j = q1 + 1; j < json.length(); j++) {
            char c = json.charAt(j);
            if (c == '\\' && j + 1 < json.length()) {
                char n = json.charAt(++j);
                sb.append(n == 'n' ? '\n' : n == 'r' ? '\r' : n == 't' ? '\t' : n);
            } else if (c == '"') {
                return sb.toString();
            } else {
                sb.append(c);
            }
        }
        return null;
    }

    static String json(boolean ok, String error, String redirect) {
        String r = redirect == null ? "" : ",\"redirect\":\"" + escape(redirect) + "\"";
        return ok ? "{\"ok\":true}" : "{\"ok\":false,\"error\":\"" + escape(error) + "\"" + r + "}";
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c == '\n') sb.append("\\n");
            else if (c == '\r') sb.append("\\r");
            else if (c == '\t') sb.append("\\t");
            else if (c < 0x20) sb.append(' ');
            else sb.append(c);
        }
        return sb.toString();
    }
}
