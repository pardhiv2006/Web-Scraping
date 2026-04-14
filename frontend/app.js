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
    let activeHistoryId = null;         // currently displayed history item id
    let isViewingHistory = false;       // whether table is showing cached history data
    let historyResults = [];            // complete dataset for current history item
    let historyMeta = { total: 0, pages: 1, limit: 50 };
    let currentScrapeHistoryId = null;  // history ID for the active scrape session

    let currentLimit = 50;
    const API_BASE = '/api';

    // --- Modal Logic ---
    const confirmModal = document.getElementById('confirm-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalMessage = document.getElementById('modal-message');
    const modalConfirmBtn = document.getElementById('modal-confirm-btn');
    const modalCancelBtn = document.getElementById('modal-cancel-btn');
    const modalCloseX = document.getElementById('modal-close-x');

    /**
     * Shows a custom confirmation modal and returns a promise
     */
    function showConfirmModal(title, message) {
        return new Promise((resolve) => {
            modalTitle.textContent = title;
            modalMessage.textContent = message;
            confirmModal.classList.remove('hidden');

            const cleanup = (result) => {
                confirmModal.classList.add('hidden');
                modalConfirmBtn.onclick = null;
                modalCancelBtn.onclick = null;
                modalCloseX.onclick = null;
                resolve(result);
            };

            modalConfirmBtn.onclick = () => cleanup(true);
            modalCancelBtn.onclick = () => cleanup(false);
            modalCloseX.onclick = () => cleanup(false);
        });
    }

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

    // ─────────────────────────── API Calls & Data Loading ───────────────────────────

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
                        clearHistoryMode();
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
        clearHistoryMode();
        
        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');
        tableContainer.classList.add('hidden');
        statsBar.classList.add('hidden');
        exportCsvBtn.classList.add('hidden');
        statTotal.textContent = '0';
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
        if (isViewingHistory) {
            console.warn("[SCRAPE] loadBusinesses blocked: currently in history mode.");
            return;
        }

        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');

        if (!selectedCountry || selectedStates.size === 0) {
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
            params.append('strict', 'false'); // Always fetch all records, not just fully enriched ones
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
        noData.classList.add('hidden');
        
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
                <td>${(b.website && typeof b.website === 'string') ? `<a href="${b.website.startsWith('http') ? b.website : 'https://' + b.website}" target="_blank" class="link-primary" style="font-size: 0.8rem; word-break: break-all;">${b.website.replace(/^https?:\/\/(www\.)?/, '').split('/')[0]} ⇗</a>` : '<span class="text-muted">-</span>'}</td>
                <td>${b.ceo_name ? `<div style="font-size: 0.85rem;">${b.ceo_name}</div>` : '<span class="text-muted">-</span>'}</td>
                <td>${b.ceo_email ? `<a href="mailto:${b.ceo_email}" class="link-primary" style="font-size: 0.8rem;">${b.ceo_email}</a>` : '<span class="text-muted">-</span>'}</td>
                <td><span style="font-size: 0.8rem; color: var(--text-dim);">${b.industry || '-'}</span></td>
                <td><span style="font-size: 0.8rem;">${b.employee_count || b.employee_size || '-'}</span></td>
                <td><span style="font-size: 0.8rem; font-weight: 600; color: #10b981;">${b.revenue || '-'}</span></td>
                <td>${(b.linkedin_url && typeof b.linkedin_url === 'string')
                    ? `<a href="${b.linkedin_url.startsWith('http') ? b.linkedin_url : 'https://' + b.linkedin_url}" target="_blank" class="link-primary" style="font-size: 0.8rem;">LinkedIn ⇗</a>`
                    : '<span class="text-muted">-</span>'}</td>
                <td>
                    ${b.source_url ? `<a href="${b.source_url}" target="_blank" class="btn-secondary" style="font-size: 10px; padding: 4px 8px;">Source ⇗</a>` : '-'}
                </td>
            `;
            businessList.appendChild(tr);
        });
    }

    // ─────────────────────────── History ───────────────────────────

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

            history.forEach(item => renderHistoryItem(item));
        } catch (err) {
            console.error('Failed to load history', err);
        }
    }

    function renderHistoryItem(item) {
        const div = document.createElement('div');
        div.className = 'history-item';
        div.id = `history-item-${item.id}`;
        if (item.id === activeHistoryId) div.classList.add('active');

        const date = new Date(item.searched_at);
        const dateStr = date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
        const timeStr = date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
        const statesLabel = item.states && item.states.length > 0
            ? (item.states.length === 1 ? item.states[0] : `${item.states.length} regions`)
            : 'All';

        div.innerHTML = `
            <div class="history-item-header">
                <span class="history-country">${item.country}</span>
                <div class="history-item-actions">
                    <button class="history-item-delete" title="Delete this entry" data-id="${item.id}">🗑️</button>
                </div>
            </div>
            <div class="history-details">${statesLabel} · ${item.result_count} results</div>
            <div class="history-item-footer">
                <span class="history-date">${dateStr} ${timeStr}</span>
            </div>
        `;

        // Click on item body (not delete button) → load cached data
        div.addEventListener('click', (e) => {
            if (e.target.closest('.history-item-delete')) return;
            loadHistoryItem(item.id, div);
        });

        // Per-item delete
        div.querySelector('.history-item-delete').addEventListener('click', async (e) => {
            e.stopPropagation();
            await deleteHistoryItem(item.id, div);
        });

        historyList.appendChild(div);
    }

    async function loadHistoryItem(historyId, divEl) {
        // Strict: terminate any active live scrape before loading history
        stopActiveScrape();

        // Visual feedback - sidebar
        divEl.classList.add('history-item-loading');
        
        // Immediate UI preparation (Instant Feedback)
        isViewingHistory = true;
        activeHistoryId = historyId;
        
        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');
        
        if (tableContainer) {
            tableContainer.classList.remove('hidden');
            tableLoading.classList.remove('hidden');
            noData.classList.add('hidden');
            businessList.innerHTML = ''; // Clear previous results instantly
            tableContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
        if (statsBar) {
            statsBar.classList.remove('hidden');
            statTotal.textContent = '...';
            statRecent.textContent = '-';
            statSkipped.textContent = '-';
        }

        try {
            const res = await fetch(`${API_BASE}/history/${historyId}`, {
                headers: Auth.getAuthHeader()
            });

            if (!res.ok) {
                showToast('Could not retrieve history data.', 'error');
                tableLoading.classList.add('hidden');
                return;
            }

            const item = await res.json();

            if (item.data_error) {
                showToast('Data corrupted for this search entry.', 'error');
                tableLoading.classList.add('hidden');
                return;
            }

            // Mark sidebar item as active
            document.querySelectorAll('.history-item').forEach(el => el.classList.remove('active'));
            divEl.classList.add('active');

            // Store in local history state for navigation
            historyResults = item.result_data || [];
            
            historyMeta = {
                total: item.pagination_meta?.total || historyResults.length,
                limit: 50,
                pages: item.pagination_meta?.pages || Math.ceil(historyResults.length / 50) || 1
            };
            currentPage = 1; 

            // Render first page from snapshot
            const initialPage = historyResults.slice(0, historyMeta.limit);
            renderTable(initialPage);
            
            statTotal.textContent = historyMeta.total;

            if (tableContainer) {
                const tableHeaderTitle = tableContainer.querySelector('.table-header h2');
                if (tableHeaderTitle) {
                    const d = new Date(item.searched_at);
                    tableHeaderTitle.innerHTML = `Historical Data <span class="table-source-tag">from ${d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}</span>`;
                }
            }

            // Pagination Display
            pageInfo.textContent = `Page ${currentPage} of ${historyMeta.pages}`;
            prevPageBtn.disabled = true;
            nextPageBtn.disabled = historyMeta.pages <= 1;

            showToast(`Loaded ${historyMeta.total} records from ${new Date(item.searched_at).toLocaleDateString()}`);
        } catch (err) {
            console.error('Failed to load history item', err);
            showToast('Failed to load cached results.', 'error');
        } finally {
            divEl.classList.remove('history-item-loading');
            tableLoading.classList.add('hidden');
        }
    }

    function clearHistoryMode() {
        // Reset table header, pagination, and history state back to "live" mode
        isViewingHistory = false;
        activeHistoryId = null;
        document.querySelectorAll('.history-item').forEach(el => el.classList.remove('active'));
        const tableHeaderTitle = document.querySelector('.data-table-container .table-header h2');
        if (tableHeaderTitle) {
            tableHeaderTitle.innerHTML = 'Extracted Data';
            tableHeaderTitle.classList.remove('historical-view');
        }
    }

    function stopActiveScrape() {
        if (scrapeInterval) {
            console.log("[SCRAPE] Stopping active enrichment polling...");
            clearInterval(scrapeInterval);
            scrapeInterval = null;
        }
        currentScrapeHistoryId = null;
        startScrapeBtn.disabled = false;
        startScrapeBtn.querySelector('.btn-text').textContent = 'Start Scrape';
        checkScrapeButton();
    }

    async function deleteHistoryItem(historyId, divEl) {
        const confirmed = await showConfirmModal(
            'Confirm Deletion',
            'Are you sure you want to permanently delete this search history entry? This action cannot be undone.'
        );
        if (!confirmed) return;
        try {
            const res = await fetch(`${API_BASE}/history/${historyId}`, {
                method: 'DELETE',
                headers: Auth.getAuthHeader()
            });
            if (!res.ok) {
                showToast('Failed to delete history item.', 'error');
                return;
            }
            // Animate out then remove
            divEl.style.transition = 'opacity 0.25s, transform 0.25s';
            divEl.style.opacity = '0';
            divEl.style.transform = 'translateX(-20px)';
            setTimeout(() => {
                divEl.remove();
                if (historyList.children.length === 0) {
                    historyList.innerHTML = '<div class="empty-history">No history yet</div>';
                }
            }, 260);

            // If we were viewing this item's data, clear the view
            if (activeHistoryId === historyId) {
                clearHistoryMode();
                const tableContainer = document.querySelector('.data-table-container');
                if (tableContainer) tableContainer.classList.add('hidden');
                document.querySelector('.stats-bar')?.classList.add('hidden');
            }
        } catch (err) {
            console.error('Failed to delete history item', err);
            showToast('Failed to delete history item.', 'error');
        }
    }

    // Removed saveSearch: History snapshots are now handled directly by the backend for 100% integrity.

    // ─────────────────────────── UI Helpers ───────────────────────────

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
        if (!container) {
            console.warn("⚠️ Toast container missing. Message:", message);
            return;
        }
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

        // ── Reset UI ─────────────────────────────────────────────────────────
        stopActiveScrape();
        clearHistoryMode();
        currentPage = 1;
        historyResults = [];
        businessList.innerHTML = '';
        statRecent.textContent  = '0';
        statSkipped.textContent = '0';
        statTotal.textContent   = '0';

        startScrapeBtn.disabled = true;
        startScrapeBtn.querySelector('.btn-text').textContent = 'Extracting...';

        const tableContainer = document.querySelector('.data-table-container');
        const statsBar = document.querySelector('.stats-bar');

        if (tableContainer) {
            tableContainer.classList.remove('hidden');
            tableContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
        if (statsBar) {
            statsBar.classList.remove('hidden');
            statRecent.textContent  = '...';
            statSkipped.textContent = '...';
        }

        tableLoading.classList.remove('hidden');
        noData.classList.add('hidden');
        if (exportCsvBtn) exportCsvBtn.classList.add('hidden');

        const startTime = Date.now();

        try {
            const res = await fetch(`${API_BASE}/scrape`, {
                method: 'POST',
                headers: { ...Auth.getAuthHeader(), 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    country: selectedCountry,
                    states: Array.from(selectedStates.keys())
                })
            });

            const data = await res.json();
            console.log("📊 [Scrape Response]:", data);
            const duration = ((Date.now() - startTime) / 1000).toFixed(1);

            if (res.ok && data.success) {
                const s = data.summary;
                const records = s.records || [];

                // ── Update stats ──────────────────────────────────────────────
                statRecent.textContent  = s.inserted  ?? 0;
                statSkipped.textContent = s.skipped_dupes ?? 0;
                statTotal.textContent   = records.length;

                // ── Store result set for pagination ──────────────────────────
                isViewingHistory = true;          // use local pagination, not server
                activeHistoryId  = s.history_id;
                historyResults   = records;
                historyMeta = {
                    total : records.length,
                    limit : 50,
                    pages : Math.max(1, Math.ceil(records.length / 50))
                };

                // ── INSTANT TABLE RENDER (page 1) ─────────────────────────────
                renderTable(records.slice(0, 50));
                renderPagination();

                // ── Export button ─────────────────────────────────────────────
                if (records.length > 0 && exportCsvBtn) {
                    exportCsvBtn.classList.remove('hidden');
                }

                // ── Table header tag ──────────────────────────────────────────
                const tableHeaderTitle = tableContainer && tableContainer.querySelector('.table-header h2');
                if (tableHeaderTitle) {
                    const now = new Date();
                    tableHeaderTitle.innerHTML =
                        `Extracted Data <span class="table-source-tag">Live · ${now.toLocaleTimeString()}</span>`;
                }

                // ── Refresh sidebar (non-blocking) ────────────────────────────
                loadHistory();

                if (records.length === 0) {
                    showToast('Scrape completed — no quality records found for selected region.', 'error');
                } else {
                    showToast(`✓ ${records.length} records loaded in ${duration}s.`, 'success');
                }

            } else {
                // API returned an error structure
                showToast(data.detail || 'Scraping failed — please try again.', 'error');
                noData.classList.remove('hidden');
            }

        } catch (error) {
            console.error('[Scrape] Network error:', error);
            showToast('Network error during extraction — is the server running?', 'error');
            noData.classList.remove('hidden');
        } finally {
            // Always restore button and hide spinner
            startScrapeBtn.querySelector('.btn-text').textContent = 'Start Scrape';
            tableLoading.classList.add('hidden');
            checkScrapeButton(); // re-enables button based on selections
        }
    }

    function renderPagination() {
        if (isViewingHistory) {
            pageInfo.textContent = `Page ${currentPage} of ${historyMeta.pages}`;
            prevPageBtn.disabled = currentPage <= 1;
            nextPageBtn.disabled = currentPage >= historyMeta.pages;
        } else {
            // Live pagination is handled inside loadBusinesses
        }
    }

    // ─────────────────────────── Event Listeners ───────────────────────────

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

        refreshBtn.addEventListener('click', () => {
            clearHistoryMode();
            loadBusinesses(1);
        });

        prevPageBtn.addEventListener('click', () => {
            if (isViewingHistory) {
                if (currentPage > 1) {
                    currentPage--;
                    const start = (currentPage - 1) * historyMeta.limit;
                    const end = start + historyMeta.limit;
                    renderTable(historyResults.slice(start, end));
                    renderPagination();
                }
            } else {
                if (currentPage > 1) loadBusinesses(currentPage - 1);
            }
        });

        nextPageBtn.addEventListener('click', () => {
            if (isViewingHistory) {
                if (currentPage < historyMeta.pages) {
                    currentPage++;
                    const start = (currentPage - 1) * historyMeta.limit;
                    const end = start + historyMeta.limit;
                    renderTable(historyResults.slice(start, end));
                    renderPagination();
                }
            } else {
                loadBusinesses(currentPage + 1);
            }
        });

        searchInput.addEventListener('input', (e) => {
            const term = e.target.value.toLowerCase();
            document.querySelectorAll('#business-list tr').forEach(row => {
                row.style.display = row.textContent.toLowerCase().includes(term) ? '' : 'none';
            });
        });

        // Clear ALL history — permanently deletes from DB
        clearHistoryBtn.addEventListener('click', async () => {
            const confirmed = await showConfirmModal(
                'Clear All History',
                'Are you sure you want to permanently delete ALL search history? This cannot be undone.'
            );
            if (!confirmed) return;
            try {
                const res = await fetch(`${API_BASE}/history`, {
                    method: 'DELETE',
                    headers: Auth.getAuthHeader()
                });
                if (res.ok) {
                    historyList.innerHTML = '<div class="empty-history">No history yet</div>';
                    clearHistoryMode();
                    showToast('Search history cleared.');
                } else {
                    showToast('Failed to clear history.', 'error');
                }
            } catch (err) {
                console.error('Failed to clear history', err);
                showToast('Failed to clear history.', 'error');
            }
        });
    }
});
