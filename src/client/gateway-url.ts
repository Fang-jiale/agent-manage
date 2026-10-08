/** Accept the address users see in their browser, retaining a reverse-proxy prefix. */
export function normalizeGateway(input: string): string {
  let url: URL;
  try { url = new URL(input.trim()); } catch { throw new Error("请填写完整的组织地址，例如 https://agentmatrix.top:8443"); }
  if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol) || url.username || url.password) {
    throw new Error("组织地址只支持 HTTP(S) 或 WS(S)，且不能包含用户名或密码");
  }
  url.protocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
  url.search = "";
  url.hash = "";
  let prefix = url.pathname.replace(/\/+$/, "");
  if (prefix.endsWith("/ws/agent")) prefix = prefix.slice(0, -9);
  url.pathname = prefix + "/ws/agent";
  return url.toString();
}

export function gatewayBase(input: string): string {
  const url = new URL(normalizeGateway(input));
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname = url.pathname.slice(0, -9) || "/";
  return url.toString().replace(/\/$/, "");
}
