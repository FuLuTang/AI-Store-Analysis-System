(function () {
    const AUTH_TOKEN_KEY = 'authToken';
    const AUTH_TOKEN_COOKIE = 'authToken';
    const AUTH_TOKEN_MAX_AGE = 12 * 60 * 60;

    function getCookie(name) {
        const prefix = `${encodeURIComponent(name)}=`;
        const parts = String(document.cookie || '').split(/;\s*/);
        for (const part of parts) {
            if (!part || !part.startsWith(prefix)) continue;
            return decodeURIComponent(part.slice(prefix.length));
        }
        return '';
    }

    function buildCookieSuffix() {
        const parts = ['Path=/', 'SameSite=Lax', `Max-Age=${AUTH_TOKEN_MAX_AGE}`];
        if (window.location.protocol === 'https:') {
            parts.push('Secure');
        }
        return parts.join('; ');
    }

    function setCookie(name, value) {
        document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}; ${buildCookieSuffix()}`;
    }

    function clearCookie(name) {
        const parts = ['Path=/', 'SameSite=Lax', 'Max-Age=0'];
        if (window.location.protocol === 'https:') {
            parts.push('Secure');
        }
        document.cookie = `${encodeURIComponent(name)}=; ${parts.join('; ')}`;
    }

    function hydrateAuthTokenFromCookie() {
        const current = sessionStorage.getItem(AUTH_TOKEN_KEY) || '';
        if (current) {
            return current;
        }
        const token = getCookie(AUTH_TOKEN_COOKIE);
        if (token) {
            sessionStorage.setItem(AUTH_TOKEN_KEY, token);
        }
        return token;
    }

    function getAuthToken() {
        return hydrateAuthTokenFromCookie();
    }

    function persistAuthToken(token) {
        const value = String(token || '').trim();
        if (!value) {
            clearAuthToken();
            return '';
        }
        sessionStorage.setItem(AUTH_TOKEN_KEY, value);
        setCookie(AUTH_TOKEN_COOKIE, value);
        return value;
    }

    function clearAuthToken() {
        sessionStorage.removeItem(AUTH_TOKEN_KEY);
        clearCookie(AUTH_TOKEN_COOKIE);
    }

    window.bootstrapAuthTokenFromCookie = hydrateAuthTokenFromCookie;
    window.getAuthToken = getAuthToken;
    window.persistAuthToken = persistAuthToken;
    window.clearAuthToken = clearAuthToken;

    hydrateAuthTokenFromCookie();
})();
