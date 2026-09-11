function(credentials) {
  // This function is shared by the Rust driver and the IEEE Python client.
  // Never return page text, URLs, account names, or secrets to the task log.
  const trusted = (url) => url.protocol === 'https:' && url.hostname === 'd.buaa.edu.cn'
    && (!url.port || url.port === '443') && !url.username && !url.password;
  const loginPath = path => path === '/login'
    || path === '/https/77726476706e69737468656265737421e3e44ed225256951300d8db9d6562d/login';
  if (!trusted(new URL(location.href))) return {state: 'untrusted'};
  const visible = el => !!el && !el.disabled && el.getClientRects().length > 0
    && getComputedStyle(el).visibility !== 'hidden' && getComputedStyle(el).display !== 'none';
  const inputs = [...document.querySelectorAll('input')];
  const text = (document.body?.innerText || '').slice(0, 16000);
  if (inputs.some(el => visible(el) && /captcha|verifycode|otp|验证码|动态码/i.test(
    [el.name, el.id, el.placeholder, el.autocomplete].join(' ')))
    || /请输入.{0,5}验证码|滑动验证|扫码确认|短信验证码|动态口令|二次验证|多因素认证|two.factor|one.time code|captcha/i.test(text)
    || [...document.querySelectorAll('iframe')].some(el => visible(el) && /captcha|turnstile|challenge/i.test(el.src))) {
    return {state: 'manual_required'};
  }
  const form = document.querySelector('form#loginForm');
  const onLoginPath = loginPath(location.pathname);
  if (!form) {
    if (onLoginPath || /统一身份认证/.test(document.title)
      || inputs.some(el => el.type === 'password')) return {state: 'loading'};
    const gateway = /^\/(?:https|http)\/[a-f0-9]+(?:\/|$)/i.test(location.pathname);
    const logout = [...document.querySelectorAll('a,button')]
      .some(el => visible(el) && /退出|注销|logout/i.test(el.textContent));
    const resourceHome = location.pathname === '/'
      && document.title === '北京航空航天大学资源访问系统 - 资源站点'
      && [...document.querySelectorAll('a')].some(el => el.textContent.trim() === '注销');
    return {state: document.readyState === 'complete' && (gateway || logout || resourceHome) ? 'ready' : 'loading'};
  }
  const action = new URL(form.action, location.href);
  // WebVPN exposes the original CAS action through its DOM URL rewriting.
  const originalCas = action.protocol === 'https:' && action.hostname === 'sso.buaa.edu.cn'
    && (!action.port || action.port === '443') && !action.username && !action.password
    && action.pathname === '/login';
  if (!onLoginPath || !((trusted(action) && loginPath(action.pathname)) || originalCas)
    || String(form.method).toLowerCase() !== 'post') return {state: 'untrusted'};
  if ((document.querySelector('#errorDiv')?.textContent || '').trim()
    || /密码错误|用户名或密码不正确|账号.{0,8}锁定|invalid credentials|incorrect password/i.test(text)) {
    return {state: 'rejected'};
  }
  const account = form.querySelector('input[name="username"]');
  const password = form.querySelector('input[name="password"][type="password"]');
  const submit = form.querySelector('input[type="submit"],button[type="submit"]');
  if (!account || !password || !submit) return {state: 'loading'};
  if (!credentials) return {state: 'credentials_required'};
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
  for (const [element, value] of [[account, credentials.account], [password, credentials.password]]) {
    setter.call(element, value);
    element.dispatchEvent(new Event('input', {bubbles: true}));
    element.dispatchEvent(new Event('change', {bubbles: true}));
  }
  // Preserve hidden CAS execution/CSRF fields and the site's own handlers.
  submit.click();
  return {state: 'submitted'};
}
