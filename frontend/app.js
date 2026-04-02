document.addEventListener('DOMContentLoaded', async () => {
    // --- Auth Check ---
    await Auth.checkAuth();
    
    // --- Elements ---
    const navUsername = document.getElementById('nav-username');
    const logoutBtn = document.getElementById('logout-btn');
    const user = Auth.getUser();
    if (user) navUsername.textContent = user.username;

    const countryContainer = document.getElementById('country-container');
    const countrySelectTrigger = document.getElementById('country-select-trigger');
    const countrySelectLabel = document.getElementById('country-select-label');
    const countryOptions = document.getElementById('country-options');

    const stateSelectContainer = document.querySelector('.multi-select-container:not(#country-container)');
    const stateSelectTrigger = document.getElementById('state-select-trigger');
    const stateSelectLabel = document.getElementById('state-select-label');
    const stateOptions = document.getElementById('state-options');

    const startScrapeBtn = document.getElementById('start-scrape-btn');
    const exportCsvBtn = document.getElementById('export-csv-btn');
    const businessList = document.getElementById('business-list');
    const noData = document.getElementById('no-data');
    const tableLoading = document.getElementById('table-loading');
    const refreshBtn = document.getElementById('refresh-btn');
    const searchInput = document.getElementById('table-search');
    
    // History elements
    const historyList = document.getElementById('history-list');
    const clearHistoryBtn = document.getElementById('clear-history-btn');

    // Stats elements
    const statTotal = document.getElementById('stat-total');
    const statRecent = document.getElementById('stat-recent');
    const statSkipped = document.getElementById('stat-skipped');

    // Pagination elements
    const prevPageBtn = document.getElementById('prev-page');
    const nextPageBtn = document.getElementById('next-page');
    const pageInfo = document.getElementById('page-info');

    // --- State ---
    let selectedCountry = null;
    let selectedStates = new Map();
    let currentPage = 1;

    let currentLimit = 50;
    const API_BASE = '/api';

    // --- Initialization ---
    init();

    async function init() {
        console.log("🚀 Initializing BizScraper AI Dashboard...");
        setupEventListeners();
        
        try {
            await Promise.all([
                loadCountries(),
                loadHistory()
            ]);
        } catch (error) {
            console.error("❌ Initialization data load failed:", error);
        }
    }

    // --- API Calls & Data Loading ---
    async function loadCountries() {
        try {
            const res = await fetch(`${API_BASE}/countries`, { headers: Auth.getAuthHeader() });
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            
            const data = await res.json();
            countryOptions.innerHTML = '';
            
            if (!data.countries || data.countries.length === 0) {
                countryOptions.innerHTML = '<div style="padding: 15px; color: var(--text-dim); text-align: center;">No countries found</div>';
                return;
            }

            data.countries.forEach(c => {
                const div = document.createElement('div');
                div.className = 'option-item';
                div.innerHTML = `
                    <input type="checkbox" id="country-${c.code}" value="${c.code}" class="country-checkbox">
                    <label for="country-${c.code}">${c.name}</label>
                `;
                
                div.addEventListener('click', (e) => {
                    const checkbox = div.querySelector('input');
                    if (e.target.tagName !== 'INPUT') checkbox.checked = !checkbox.checked;
                    
                    if (checkbox.checked) {
                        document.querySelectorAll('.country-checkbox').forEach(cb => {
                            if (cb !== checkbox) {
                                cb.checked = false;
                                cb.parentElement.classList.remove('selected');
                            }
                        });
                        handleCountrySelect(c);
                        div.classList.add('selected');
                    } else {
                        selectedCountry = null;
                        div.classList.remove('selected');
                        resetStates();
                        loadBusinesses(1);
                    }
                });
                countryOptions.appendChild(div);
            });
        } catch (error) {
            console.error("❌ Failed to load countries:", error);
            showToast('API Connection Error: Failed to load countries.', 'error');
        }
    }

    function handleCountrySelect(country) {
        selectedCountry = country.code;
        countrySelectLabel.textContent = country.name;
        countryOptions.classList.add('hidden');
        countrySelectTrigger.classList.remove('active');
        resetStates();
        loadStates(country.code);
        
        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');
        tableContainer.classList.add('hidden');
        statsBar.classList.add('hidden');
        exportCsvBtn.classList.add('hidden');
        statTotal.textContent = '0';
        
        // Don't call loadBusinesses here yet, wait for states
    }

    async function loadStates(countryCode) {
        try {
            stateOptions.innerHTML = '<div style="padding: 15px; color: var(--text-dim); text-align: center;"><div class="spinner" style="margin: 0 auto 10px;"></div>Loading Regions...</div>';
            const res = await fetch(`${API_BASE}/countries/${countryCode}/states`, { headers: Auth.getAuthHeader() });
            const data = await res.json();
            stateOptions.innerHTML = '';
            
            if (data.states && data.states.length > 0) {
                const selectAllDiv = document.createElement('div');
                selectAllDiv.className = 'option-item';
                selectAllDiv.innerHTML = `
                    <input type="checkbox" id="state-all" value="ALL">
                    <label for="state-all" style="font-weight: 700;">Select All Regions</label>
                `;
                selectAllDiv.addEventListener('click', (e) => {
                    const checkbox = selectAllDiv.querySelector('input');
                    if (e.target.tagName !== 'INPUT') checkbox.checked = !checkbox.checked;
                    const isChecked = checkbox.checked;
                    document.querySelectorAll('.state-checkbox').forEach(cb => {
                        cb.checked = isChecked;
                        const stateName = cb.parentElement.querySelector('label').textContent;
                        isChecked ? selectedStates.set(cb.value, stateName) : selectedStates.delete(cb.value);
                        cb.parentElement.classList.toggle('selected', isChecked);
                    });
                    updateStateLabel();
                    checkScrapeButton();
                });
                stateOptions.appendChild(selectAllDiv);

                data.states.forEach(state => {
                    const div = document.createElement('div');
                    div.className = 'option-item';
                    div.innerHTML = `
                        <input type="checkbox" id="state-${state.code}" value="${state.code}" class="state-checkbox">
                        <label for="state-${state.code}">${state.name}</label>
                    `;
                    div.addEventListener('click', (e) => {
                        const checkbox = div.querySelector('input');
                        if (e.target.tagName !== 'INPUT') checkbox.checked = !checkbox.checked;
                        if (checkbox.checked) {
                            selectedStates.set(state.code, state.name);
                            div.classList.add('selected');
                        } else {
                            selectedStates.delete(state.code);
                            div.classList.remove('selected');
                            const allCb = document.getElementById('state-all');
                            if(allCb) allCb.checked = false;
                        }
                        updateStateLabel();
                        checkScrapeButton();
                    });
                    stateOptions.appendChild(div);
                });
            } else {
                stateOptions.innerHTML = '<div style="padding: 15px; color: var(--text-dim); text-align: center;">No regions available</div>';
            }
        } catch (error) {
            console.error("❌ Failed to load regions:", error);
            showToast('Failed to load regions', 'error');
        }
    }

    function resetStates() {
        selectedStates.clear();
        stateOptions.innerHTML = '<div style="padding: 15px; color: var(--text-dim); text-align: center;">Select a country first</div>';
        updateStateLabel();
        checkScrapeButton();
    }

    async function loadBusinesses(page = 1) {
        console.log(`[DEBUG] loadBusinesses called: page=${page}, country=${selectedCountry}, statesSize=${selectedStates.size}`);
        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');

        if (!selectedCountry || selectedStates.size === 0) {
            console.warn("[DEBUG] loadBusinesses exiting early: Missing country or states");
            if (tableContainer) tableContainer.classList.add('hidden');
            if (statsBar) statsBar.classList.add('hidden');
            exportCsvBtn.classList.add('hidden');
            statTotal.textContent = '0';
            return;
        }

        try {
            tableLoading.classList.remove('hidden');
            noData.classList.add('hidden');
            
            const params = new URLSearchParams();
            params.append('page', page);
            params.append('limit', currentLimit);
            params.append('country', selectedCountry.toUpperCase());
            Array.from(selectedStates.keys()).forEach(state => {
                params.append('state', state.toUpperCase());
            });

            const url = `${API_BASE}/businesses?${params.toString()}`;
            const res = await fetch(url, { headers: Auth.getAuthHeader() });
            if (!res.ok) throw new Error(`Fetch failed with status ${res.status}`);
            
            const data = await res.json();
            
            renderTable(data.businesses);
            statTotal.textContent = data.total;
            currentPage = data.page;
            
            if (tableContainer) tableContainer.classList.remove('hidden');
            if (statsBar) statsBar.classList.remove('hidden');
            
            if (data.total > 0) {
                if (exportCsvBtn) exportCsvBtn.classList.remove('hidden');
            } else {
                if (exportCsvBtn) exportCsvBtn.classList.add('hidden');
            }

            const totalPages = Math.ceil(data.total / currentLimit) || 1;
            pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
            prevPageBtn.disabled = currentPage <= 1;
            nextPageBtn.disabled = currentPage >= totalPages;
        } catch (error) {
            console.error("❌ [DEBUG] Failed to load businesses:", error);
            showToast('Failed to load businesses from server', 'error');
        } finally {
            tableLoading.classList.add('hidden');
        }
    }

    function renderTable(businesses) {
        businessList.innerHTML = '';
        if (!businesses || businesses.length === 0) {
            noData.classList.remove('hidden');
            return;
        }
        
        businesses.forEach(b => {
            const tr = document.createElement('tr');
            let statusClass = (b.status || '').toLowerCase().includes('active') ? 'status-active' : 'status-pending';
            
            tr.innerHTML = `
                <td><strong>${b.company_name}</strong></td>
                <td><span style="font-family: monospace; opacity: 0.7;">${b.registration_number}</span></td>
                <td><span style="font-size: 0.85rem;">${b.registration_date || '-'}</span></td>
                <td><span class="status-badge ${statusClass}">${b.status || 'Active'}</span></td>
                <td><span style="font-size: 0.85rem; display: block; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${b.address || ''}">${b.address || '-'}</span></td>
                <td>${b.country || '-'}</td>
                <td>${b.state || '-'}</td>
                <td>${b.email || '<span class="text-muted">-</span>'}</td>
                <td>${b.phone || '<span class="text-muted">-</span>'}</td>
                <td>${b.website ? `<a href="${b.website.startsWith('http') ? b.website : 'https://' + b.website}" target="_blank" class="link-primary" style="font-size: 0.8rem; word-break: break-all;">${b.website.replace(/^https?:\/\/(www\.)?/, '').split('/')[0]} ⇗</a>` : '<span class="text-muted">-</span>'}</td>
                <td>
                    <div style="font-size: 0.85rem;">
                        ${b.ceo_name ? `<div>${b.ceo_name}</div>` : ''}
                        ${b.founder_name && b.founder_name !== b.ceo_name ? `<div class="text-muted" style="font-size: 0.75rem;">Founder: ${b.founder_name}</div>` : ''}
                        ${!b.ceo_name && !b.founder_name ? '<span class="text-muted">-</span>' : ''}
                    </div>
                </td>
                <td>${b.ceo_email
                    ? `<a href="mailto:${b.ceo_email}" class="link-primary" style="font-size: 0.8rem;">${b.ceo_email}</a>`
                    : '<span class="text-muted">-</span>'}</td>
                <td>${b.linkedin_url
                    ? `<a href="${b.linkedin_url}" target="_blank" class="link-primary" style="font-size: 0.8rem;">LinkedIn ⇗</a>`
                    : '<span class="text-muted">-</span>'}</td>
                <td>
                    ${b.source_url ? `<a href="${b.source_url}" target="_blank" class="btn-secondary" style="font-size: 10px; padding: 4px 8px;">Source ⇗</a>` : '-'}
                </td>
            `;
            businessList.appendChild(tr);
        });
    }

    async function loadHistory() {
        try {
            const res = await fetch(`${API_BASE}/history`, { headers: Auth.getAuthHeader() });
            if (!res.ok) return;
            const history = await res.json();
            
            historyList.innerHTML = '';
            if (history.length === 0) {
                historyList.innerHTML = '<div class="empty-history">No history yet</div>';
                return;
            }

            history.forEach(item => {
                const div = document.createElement('div');
                div.className = 'history-item';
                const date = new Date(item.searched_at).toLocaleDateString();
                div.innerHTML = `
                    <div class="history-item-header">
                        <span class="history-country">${item.country}</span>
                        <span class="history-date">${date}</span>
                    </div>
                    <div class="history-details">${item.result_count} results found</div>
                `;
                div.addEventListener('click', () => {
                   // Quick re-run logic could go here
                });
                historyList.appendChild(div);
            });
        } catch (err) {
            console.error('Failed to load history', err);
        }
    }

    async function saveSearch(summary) {
        try {
            console.log(`[DEBUG] saving search: total_fetched=${summary.total_fetched}`);
            await fetch(`${API_BASE}/history`, {
                method: 'POST',
                headers: { ...Auth.getAuthHeader(), 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    country: selectedCountry,
                    states: Array.from(selectedStates.keys()),
                    result_count: summary.total_fetched
                })
            });
            loadHistory();
        } catch (err) {
            console.error('❌ [DEBUG] Failed to save history', err);
        }
    }

    function updateStateLabel() {
        if (selectedStates.size === 0) {
            stateSelectLabel.textContent = 'Select regions';
        } else if (selectedStates.size === 1) {
            stateSelectLabel.textContent = selectedStates.values().next().value;
        } else {
            stateSelectLabel.textContent = `${selectedStates.size} regions selected`;
        }
    }

    function checkScrapeButton() {
        startScrapeBtn.disabled = !selectedCountry || selectedStates.size === 0;
    }

    function showToast(message, type = 'success') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = 'toast';
        if (type === 'error') toast.style.borderColor = 'var(--danger)';
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(50px)';
            setTimeout(() => toast.remove(), 500);
        }, 5000);
    }

    let scrapeInterval = null;

    async function handleStartScrape() {
        if (!selectedCountry || selectedStates.size === 0) return;
        
        if (scrapeInterval) clearInterval(scrapeInterval);
        
        startScrapeBtn.disabled = true;
        startScrapeBtn.querySelector('.btn-text').textContent = 'Extracting...';
        
        // Make table and spinner visible immediately
        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');
        
        if (tableContainer) {
            tableContainer.classList.remove('hidden');
            tableContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
        if (statsBar) {
            statsBar.classList.remove('hidden');
        }
        
        tableLoading.classList.remove('hidden');
        noData.classList.add('hidden');
        
        try {
            const res = await fetch(`${API_BASE}/scrape`, {
                method: 'POST',
                headers: { ...Auth.getAuthHeader(), 'Content-Type': 'application/json' },
                body: JSON.stringify({ country: selectedCountry, states: Array.from(selectedStates.keys()) })
            });

            const data = await res.json();
            if (res.ok && data.success) {
                const s = data.summary;
                showToast(`STARTED: ${s.total_fetched} records found. Enriching fields...`);
                statRecent.textContent = s.inserted;
                statSkipped.textContent = parseInt(statSkipped.textContent || '0') + s.skipped_dupes;
                saveSearch(s);
                
                // Initial Load (Basic Data)
                await loadBusinesses(1);

                // POLL FOR ENRICHMENT (Real-time updates)
                let pollCount = 0;
                scrapeInterval = setInterval(async () => {
                    pollCount++;
                    console.log(`[POLL] Refreshing enriched data (Attempt ${pollCount})...`);
                    await loadBusinesses(currentPage);
                    if (pollCount >= 20) { // Stop after 1 minute (20 * 3s)
                        clearInterval(scrapeInterval);
                        scrapeInterval = null;
                        console.log("[POLL] Mass enrichment polling completed.");
                    }
                }, 3000);

            } else {
                showToast(data.detail || 'Scraping failed', 'error');
            }
        } catch (error) {
            showToast('Network error during extraction', 'error');
        } finally {
            startScrapeBtn.querySelector('.btn-text').textContent = 'Start Scrape';
            checkScrapeButton();
        }
    }

    function setupEventListeners() {
        logoutBtn.addEventListener('click', () => Auth.logout());
        countrySelectTrigger.addEventListener('click', (e) => {
            e.stopPropagation();
            stateOptions.classList.add('hidden');
            stateSelectTrigger.classList.remove('active');
            countryOptions.classList.toggle('hidden');
            countrySelectTrigger.classList.toggle('active');
        });

        stateSelectTrigger.addEventListener('click', (e) => {
            e.stopPropagation();
            if (!selectedCountry) {
                showToast('Please select a country first', 'error');
                return;
            }
            countryOptions.classList.add('hidden');
            countrySelectTrigger.classList.remove('active');
            stateOptions.classList.toggle('hidden');
            stateSelectTrigger.classList.toggle('active');
        });

        document.addEventListener('click', (e) => {
            if (!countryContainer.contains(e.target)) {
                countryOptions.classList.add('hidden');
                countrySelectTrigger.classList.remove('active');
            }
            if (!stateSelectContainer.contains(e.target)) {
                stateOptions.classList.add('hidden');
                stateSelectTrigger.classList.remove('active');
            }
        });

        startScrapeBtn.addEventListener('click', handleStartScrape);
        exportCsvBtn.addEventListener('click', async () => {
            if (!selectedCountry) return;
            const params = new URLSearchParams();
            params.append('country', selectedCountry.toUpperCase());
            Array.from(selectedStates.keys()).forEach(state => params.append('state', state.toUpperCase()));

            const url = `${API_BASE}/export/csv?${params.toString()}`;
            try {
                const originalText = exportCsvBtn.innerHTML;
                exportCsvBtn.disabled = true;
                exportCsvBtn.innerHTML = '<span>Preparing...</span>';

                const response = await fetch(url, { headers: Auth.getAuthHeader() });
                const blob = await response.blob();
                const downloadUrl = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = downloadUrl;
                a.download = `businesses_${selectedCountry.toLowerCase()}.csv`;
                document.body.appendChild(a);
                a.click();
                a.remove();
                
                exportCsvBtn.innerHTML = originalText;
                exportCsvBtn.disabled = false;
                showToast('CSV Exported Successfully');
            } catch (error) {
                showToast('Failed to export CSV', 'error');
                exportCsvBtn.disabled = false;
            }
        });

        refreshBtn.addEventListener('click', () => loadBusinesses(1));
        prevPageBtn.addEventListener('click', () => currentPage > 1 && loadBusinesses(currentPage - 1));
        nextPageBtn.addEventListener('click', () => loadBusinesses(currentPage + 1));

        searchInput.addEventListener('input', (e) => {
            const term = e.target.value.toLowerCase();
            document.querySelectorAll('#business-list tr').forEach(row => {
                row.style.display = row.textContent.toLowerCase().includes(term) ? '' : 'none';
            });
        });

        clearHistoryBtn.addEventListener('click', async () => {
            // Simplified clear for now (just visual or delete all in background)
            historyList.innerHTML = '<div class="empty-history">History cleared</div>';
        });
    }
});

