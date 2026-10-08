const { URL } = require('node:url');
const allowed = [
  /^\/api\/(state|activities|brands|products|product-catalog|remote-products|pair|pair\/cancel|connection\/reset|launch)$/,
  /^\/api\/settings\/products-dir$/,
  /^\/api\/agents(?:\/[A-Za-z0-9._%:-]+(?:\/retry)?)?$/,
  /^\/api\/products\/install(?:\?[^#]*)?$/,
  /^\/api\/products\/[A-Za-z0-9._%-]+(?:\/(?:instantiate|local-run\/(?:start|stop)|[A-Za-z0-9.+-]+\/(?:install-remote|update-in-place)))?$/,
];
function validateRequest(endpoint, options = {}) {
  if (typeof endpoint !== 'string' || !allowed.some(pattern => pattern.test(endpoint))) throw Error('Unsupported client request');
  const method = options.method || 'GET';
  if (!['GET', 'POST', 'PUT', 'DELETE'].includes(method)) throw Error('Unsupported method');
  let body = options.body;
  if (body != null && typeof body !== 'string' && !(body instanceof Uint8Array) && !(body instanceof ArrayBuffer)) throw Error('Unsupported request body');
  if (body instanceof ArrayBuffer) body = Buffer.from(body);
  if (body instanceof Uint8Array) body = Buffer.from(body);
  if (body && Buffer.byteLength(body) > 512 * 1024 * 1024) throw Error('安装包超过 512MB');
  return { method, body, contentType: typeof body === 'string' ? 'application/json' : 'application/octet-stream' };
}
function externalUrl(value) {
  const url = new URL(value);
  if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) throw Error('Only HTTP(S) links are allowed');
  return url.href;
}
module.exports = { validateRequest, externalUrl };
