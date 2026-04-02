const Auth = {
    API_BASE: '/api/auth',
    
    login: async (username, password) => {
        const response = await fetch(`${Auth.API_BASE}/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Login failed');
        
        localStorage.setItem('token', data.access_token);
        localStorage.setItem('user', JSON.stringify(data.user));
        return true;
    },
    
    register: async (username, email, password) => {
        const response = await fetch(`${Auth.API_BASE}/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, email, password })
        });
        
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Registration failed');
        return true;
    },
    
    logout: () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        window.location.href = '/login.html';
    },
    
    getToken: () => localStorage.getItem('token'),
    
    getUser: () => {
        const user = localStorage.getItem('user');
        return user ? JSON.parse(user) : null;
    },
    
    isLoggedIn: () => !!localStorage.getItem('token'),
    
    getAuthHeader: () => {
        const token = Auth.getToken();
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    },
    
    checkAuth: async () => {
        if (!Auth.isLoggedIn()) {
            window.location.href = '/login.html';
            return;
        }
        
        try {
            const response = await fetch(`${Auth.API_BASE}/me`, {
                headers: Auth.getAuthHeader()
            });
            if (!response.ok) {
                Auth.logout();
            }
        } catch (err) {
            console.error('Auth check failed', err);
        }
    }
};
