(function () {
    // Automatically load stylesheet if not present in head
    if (!document.querySelector('link[href="chat.css"]')) {
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = 'chat.css';
        document.head.appendChild(link);
    }

    let markedLoaded = typeof marked !== 'undefined';
    let onMarkedLoaded = null;
    let mermaidLoaded = typeof mermaid !== 'undefined';
    let onMermaidLoaded = null;
    let jszipLoaded = typeof JSZip !== 'undefined';
    let onJszipLoaded = null;

    // Dynamically load JSZip if not loaded by host page
    if (!jszipLoaded) {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/npm/jszip/dist/jszip.min.js';
        script.onload = () => {
            jszipLoaded = true;
            if (onJszipLoaded) onJszipLoaded();
        };
        document.head.appendChild(script);
    }

    // Dynamically load marked if not loaded by host page
    if (!markedLoaded) {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/npm/marked/marked.min.js';
        script.onload = () => {
            markedLoaded = true;
            if (onMarkedLoaded) onMarkedLoaded();
        };
        document.head.appendChild(script);
    }

    // Dynamically load mermaid if not loaded by host page
    if (!mermaidLoaded) {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js';
        script.onload = () => {
            mermaidLoaded = true;
            try {
                mermaid.initialize({
                    startOnLoad: false,
                    theme: (document.documentElement.classList.contains('dark-theme') || document.body.classList.contains('dark-theme')) ? 'dark' : 'default',
                    securityLevel: 'loose'
                });
            } catch (e) {
                console.error('Failed to initialize mermaid', e);
            }
            if (onMermaidLoaded) onMermaidLoaded();
        };
        document.head.appendChild(script);
    }

    // Initialize chatbot on DOM ready
    document.addEventListener('DOMContentLoaded', () => {
        // Skip chatbot injection on login page
        if (window.location.pathname.endsWith('/login.html') || window.location.pathname.endsWith('/login')) {
            return;
        }

        // Create Chatbot DOM Container
        const chatContainer = document.createElement('div');
        chatContainer.id = 'chatbot-root';
        chatContainer.innerHTML = `
            <button id="chatFab" class="chat-fab" type="button" aria-label="打开对话助手" aria-expanded="false">
                <i class="fas fa-comments"></i>
            </button>

            <section id="chatPanel" class="chat-panel" aria-hidden="true">
                <div class="chat-panel-header">
                    <div>
                        <h3><i class="fas fa-comments"></i> 对话助手 <span id="chatStatusDot" class="chat-status-dot" title="在线"></span><span id="chatStatusText" class="chat-status-text">在线</span></h3>
                        <div class="chat-run-badge">账号级聊天记录</div>
                    </div>
                    <div class="chat-header-actions">
                        <button id="chatExportBtn" class="chat-export-btn" type="button" title="导出记录" aria-label="导出记录">
                            <i class="fas fa-download"></i> <span>导出记录</span>
                        </button>
                        <button id="chatCloseBtn" class="chat-panel-close" type="button" aria-label="关闭对话助手">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                </div>
                <div id="chatMessages" class="chat-messages">
                    <div class="chat-empty-state">从这里直接开始聊天。</div>
                </div>
                <div class="chat-composer">
                    <div class="chat-attach-dropdown" style="position: relative; display: inline-block; grid-area: attach;">
                        <button id="chatAttachBtn" class="chat-icon-btn chat-attach-btn" type="button" title="添加附件" aria-label="添加附件">
                            <i class="fas fa-paperclip"></i>
                        </button>
                        <div class="chat-attach-menu" id="chatAttachMenu" style="display: none; position: absolute; bottom: 100%; left: 0; z-index: 1000; background: var(--chat-composer-bg); border: 1px solid var(--chat-composer-border); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); padding: 0.35rem 0; min-width: 120px; margin-bottom: 0.25rem;">
                            <button type="button" id="chatUploadFileOption" style="display: flex; align-items: center; gap: 0.5rem; width: 100%; border: 0; background: transparent; padding: 0.5rem 0.8rem; font-size: 0.8rem; color: var(--text); cursor: pointer; text-align: left;"><i class="fas fa-file"></i> 添加文件</button>
                            <button type="button" id="chatUploadFolderOption" style="display: flex; align-items: center; gap: 0.5rem; width: 100%; border: 0; background: transparent; padding: 0.5rem 0.8rem; font-size: 0.8rem; color: var(--text); cursor: pointer; text-align: left;"><i class="fas fa-folder-open"></i> 添加文件夹</button>
                        </div>
                    </div>
                    <textarea id="chatInput" class="chat-input" placeholder="输入你想问的问题..." rows="1"></textarea>
                    <button id="chatSendBtn" class="chat-icon-btn chat-send-btn" type="button" title="发送消息" aria-label="发送消息">
                        <i class="fas fa-paper-plane"></i>
                    </button>
                    <input type="file" id="chatAttachmentInput" hidden multiple>
                    <input type="file" id="chatAttachmentFolderInput" hidden webkitdirectory directory multiple>
                    <div id="chatAttachmentDrafts" class="chat-attachment-drafts"></div>
                </div>
            </section>
        `;
        document.body.appendChild(chatContainer);

        // DOM elements select
        const chatFab = document.getElementById('chatFab');
        const chatPanel = document.getElementById('chatPanel');
        const chatCloseBtn = document.getElementById('chatCloseBtn');
        const chatExportBtn = document.getElementById('chatExportBtn');
        const chatMessages = document.getElementById('chatMessages');
        const chatInput = document.getElementById('chatInput');
        const chatAttachmentInput = document.getElementById('chatAttachmentInput');
        const chatAttachmentFolderInput = document.getElementById('chatAttachmentFolderInput');
        const chatAttachmentDrafts = document.getElementById('chatAttachmentDrafts');
        const chatAttachBtn = document.getElementById('chatAttachBtn');
        const chatAttachMenu = document.getElementById('chatAttachMenu');
        const chatUploadFileOption = document.getElementById('chatUploadFileOption');
        const chatUploadFolderOption = document.getElementById('chatUploadFolderOption');
        const chatSendBtn = document.getElementById('chatSendBtn');
        const chatStatusDot = document.getElementById('chatStatusDot');
        const chatStatusText = document.getElementById('chatStatusText');

        // Chatbot state
        let chatMessagesCache = [];
        let chatHistoryLoaded = false;
        let chatHistoryLoadPromise = null;
        let chatPendingFiles = [];
        let lastChatUpdate = '';
        let chatPollTimer = null;

        onMarkedLoaded = () => {
            if (chatHistoryLoaded) {
                renderChatMessages();
            }
        };

        onMermaidLoaded = () => {
            if (chatHistoryLoaded) {
                renderChatMessages();
            }
        };

        // Auth helper
        function authHeaders() {
            const key = (typeof window.getAuthToken === 'function'
                ? window.getAuthToken()
                : (sessionStorage.getItem('authToken') || '')) || '';
            return { 'X-Auth-Token': key };
        }

        function clearAuthAndReturn() {
            sessionStorage.removeItem('accountName');
            sessionStorage.removeItem('authToken');
            window.location.href = '/login.html';
        }

        function escapeChatHtml(text) {
            return String(text || '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        function formatChatContent(text) {
            const safeText = escapeChatHtml(text);
            const markdownText = safeText.replace(/&gt;/g, '>');
            if (typeof marked !== 'undefined' && marked && typeof marked.parse === 'function') {
                const renderer = new marked.Renderer();
                const originalCode = renderer.code.bind(renderer);
                renderer.image = (href, title, text) => {
                    const cleanHref = href ? href.replace(/&amp;/g, '&') : '';
                    return `<img src="${cleanHref}" alt="${text || ''}" title="${title || ''}" loading="lazy" onerror="this.style.display='none'" />`;
                };
                renderer.code = (code, lang, escaped) => {
                    let rawCode = code;
                    let language = lang;
                    if (typeof code === 'object' && code !== null) {
                        rawCode = code.text;
                        language = code.lang;
                    }
                    if (language === 'mermaid') {
                        const unescapedCode = rawCode
                            .replace(/&amp;/g, '&')
                            .replace(/&lt;/g, '<')
                            .replace(/&gt;/g, '>')
                            .replace(/&quot;/g, '"')
                            .replace(/&#39;/g, "'");
                        return `<div class="mermaid">${unescapedCode}</div>`;
                    }
                    if (typeof code === 'object') {
                        return originalCode(code);
                    }
                    return originalCode(code, lang, escaped);
                };
                return marked.parse(markdownText, { renderer });
            }
            return safeText.replace(/\n/g, '<br>');
        }

        function formatFileSize(size) {
            const bytes = Number(size || 0);
            if (bytes < 1024) return `${bytes} B`;
            if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
            return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
        }

        function renderChatAttachments(attachments) {
            if (!Array.isArray(attachments) || attachments.length === 0) return '';
            return `<div class="chat-attachments">${attachments.map(item => `
                <div class="chat-attachment-chip" title="${escapeChatHtml(item.originalName || '')}">
                    <i class="fas fa-paperclip"></i>
                    <span>${escapeChatHtml(item.originalName || '附件')}</span>
                    ${item.size !== undefined ? `<small>${formatFileSize(item.size)}</small>` : ''}
                </div>
            `).join('')}</div>`;
        }

        function getChatCopyText(msg) {
            const parts = [];
            const content = String(msg && msg.content ? msg.content : '').trim();
            if (content) parts.push(content);
            if (Array.isArray(msg && msg.attachments) && msg.attachments.length > 0) {
                const names = msg.attachments
                    .map(item => String(item && item.originalName ? item.originalName : '').trim())
                    .filter(Boolean);
                if (names.length > 0) {
                    parts.push(`附件：${names.join('、')}`);
                }
            }
            return parts.join('\n');
        }

        async function copyChatText(text) {
            const value = String(text || '');
            if (!value.trim()) return false;
            if (navigator.clipboard && window.isSecureContext) {
                await navigator.clipboard.writeText(value);
                return true;
            }

            const textarea = document.createElement('textarea');
            textarea.value = value;
            textarea.setAttribute('readonly', '');
            textarea.style.position = 'fixed';
            textarea.style.opacity = '0';
            textarea.style.pointerEvents = 'none';
            textarea.style.left = '-9999px';
            document.body.appendChild(textarea);
            textarea.select();
            textarea.setSelectionRange(0, textarea.value.length);
            const copied = document.execCommand('copy');
            document.body.removeChild(textarea);
            return copied;
        }

        function flashCopyButton(btn, copied) {
            if (!btn) return;
            const icon = btn.querySelector('i');
            if (copied) {
                btn.classList.add('is-copied');
                btn.setAttribute('aria-label', '已复制');
                btn.title = '已复制';
                if (icon) icon.className = 'fas fa-check';
                window.clearTimeout(btn._chatCopyTimer);
                btn._chatCopyTimer = window.setTimeout(() => {
                    btn.classList.remove('is-copied');
                    btn.setAttribute('aria-label', '复制消息');
                    btn.title = '复制消息';
                    if (icon) icon.className = 'fas fa-copy';
                }, 1200);
            } else {
                btn.classList.add('copy-failed');
                btn.setAttribute('aria-label', '复制失败');
                btn.title = '复制失败';
                window.clearTimeout(btn._chatCopyTimer);
                btn._chatCopyTimer = window.setTimeout(() => {
                    btn.classList.remove('copy-failed');
                    btn.setAttribute('aria-label', '复制消息');
                    btn.title = '复制消息';
                    if (icon) icon.className = 'fas fa-copy';
                }, 1200);
            }
        }

        function normalizeChatRecord(record) {
            if (!record || typeof record !== 'object') return null;
            if (!record.role) return null;
            return { ...record };
        }

        function messageUpdateValue(record) {
            return String((record && (record.datetime || record.time)) || '');
        }

        function updateLastChatUpdate(messages) {
            const values = (messages || []).map(messageUpdateValue).filter(Boolean);
            if (values.length) lastChatUpdate = values[values.length - 1];
        }

        function formatChatTimestamp(record) {
            if (!record || !record.datetime) return '';
            const dt = new Date(record.datetime);
            if (Number.isNaN(dt.getTime())) return record.datetime;
            return dt.toLocaleString('zh-CN', { hour12: false });
        }

        function formatChatTokenCount(record) {
            if (!record || record.role !== 'assistant') return '';
            const value = Number(record.token_count);
            if (!Number.isFinite(value) || value <= 0) return '';
            return `token: ${Math.floor(value)}`;
        }

        function setChatMessages(messages) {
            chatMessagesCache = (messages || []).map(normalizeChatRecord).filter(Boolean);
            updateLastChatUpdate(chatMessagesCache);
            chatHistoryLoaded = true;
            renderChatMessages();
        }

        function setChatOpen(open) {
            chatPanel.classList.toggle('open', open);
            chatPanel.setAttribute('aria-hidden', open ? 'false' : 'true');
            chatFab.setAttribute('aria-expanded', open ? 'true' : 'false');
            document.body.classList.toggle('chatbot-open', open);
            if (open) {
                loadChatHistory().catch((err) => {
                    console.error('Failed to load chatbot history', err);
                });
                setTimeout(() => chatInput.focus(), 50);
            }
        }

        function renderChatMessages() {
            if (!chatHistoryLoaded) {
                chatMessages.innerHTML = '<div class="chat-empty-state">正在加载聊天记录...</div>';
                return;
            }

            const history = chatMessagesCache.filter(msg => {
                if (msg.role === 'notice') return String(msg.content || '').trim().length > 0;
                if (msg.role === 'card') return true;
                if (msg.role === 'user') return true;
                if (msg.role !== 'assistant') return false;
                return String(msg.content || '').trim().length > 0;
            });
            if (history.length === 0) {
                chatMessages.innerHTML = '<div class="chat-empty-state">从这里直接开始聊天。</div>';
                return;
            }

            chatMessages.innerHTML = history.map(msg => {
                if (msg.role === 'notice') {
                    return `
                        <div class="chat-notice">
                            <div class="chat-notice-text">${escapeChatHtml(String(msg.content || '')).replace(/\n/g, '<br>')}</div>
                        </div>
                    `;
                }
                if (msg.role === 'card') {
                    return renderChatCard(msg);
                }
                let content = msg.content || '';
                const token = (typeof window.getAuthToken === 'function'
                    ? window.getAuthToken()
                    : (sessionStorage.getItem('authToken') || '')) || '';
                content = content.replace(/\{\{AUTH_TOKEN\}\}/g, token);
                const rendered = formatChatContent(content);
                const hasTable = rendered.includes('<table');
                const hasMermaid = rendered.includes('class="mermaid"') || String(msg.content || '').includes('```mermaid');
                const hasImage = rendered.includes('<img') || 
                                  /!\[.*?\]\(.*?\)/.test(msg.content || '') || 
                                  /\[.*?\]\(.*?\.(?:png|jpg|jpeg|gif|webp|svg|bmp)(?:\?.*?)?\)/i.test(msg.content || '');
                const isWide = hasTable || hasMermaid || hasImage;
                const timestamp = formatChatTimestamp(msg);
                const tokenCount = formatChatTokenCount(msg);
                const metaText = [timestamp, tokenCount].filter(Boolean).join(' ');
                return `
                <div class="chat-message ${msg.role === 'user' ? 'chat-message-user' : 'chat-message-assistant'}">
                    <div class="chat-message-role">
                        ${msg.role === 'user' ? '你' : 'AI'}
                        ${metaText ? ` · ${metaText}` : ''}
                    </div>
                    <div class="chat-message-row ${msg.role === 'user' ? 'chat-message-row-user' : 'chat-message-row-assistant'}">
                        ${msg.role === 'user' ? `
                            <button type="button" class="chat-message-copy-btn" title="复制消息" aria-label="复制消息" data-chat-copy-text="${escapeChatHtml(getChatCopyText(msg))}">
                                <i class="fas fa-copy"></i>
                            </button>
                        ` : ''}
                        <div class="chat-message-bubble ${isWide ? 'chat-message-bubble-wide' : ''}">
                            ${msg.role === 'assistant' ? `
                                <button type="button" class="chat-message-copy-btn" title="复制消息" aria-label="复制消息" data-chat-copy-text="${escapeChatHtml(getChatCopyText(msg))}">
                                    <i class="fas fa-copy"></i>
                                </button>
                            ` : ''}
                            <div class="markdown-body chat-markdown">${rendered}</div>
                            ${renderChatAttachments(msg.attachments)}
                        </div>
                    </div>
                </div>
                `;
            }).join('');
            bindChatCardActions();
            bindChatMessageActions();
            if (typeof mermaid !== 'undefined' && mermaid && typeof mermaid.run === 'function') {
                try {
                    const isDark = document.documentElement.classList.contains('dark-theme') || document.body.classList.contains('dark-theme');
                    mermaid.initialize({
                        startOnLoad: false,
                        theme: isDark ? 'dark' : 'default',
                        securityLevel: 'loose'
                    });
                } catch (e) {
                    console.error('Failed to re-initialize mermaid', e);
                }
                mermaid.run({
                    querySelector: '#chatMessages .mermaid'
                }).catch(err => console.error('Mermaid render error', err));
            }
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function renderChatCard(msg) {
            const options = Array.isArray(msg.options) ? msg.options : [];
            const choice = msg.choice || '';
            const buttons = (choice ? [choice] : options).map(option => `
                <button type="button"
                    class="chat-card-option ${choice === option ? 'selected' : ''}"
                    data-card-name="${escapeChatHtml(msg.name || '')}"
                    data-card-choice="${escapeChatHtml(option)}"
                    ${choice ? 'disabled' : ''}>
                    ${escapeChatHtml(option)}
                </button>
            `).join('');
            let detail = msg.detail || '';
            const token = (typeof window.getAuthToken === 'function'
                ? window.getAuthToken()
                : (sessionStorage.getItem('authToken') || '')) || '';
            detail = detail.replace(/\{\{AUTH_TOKEN\}\}/g, token);
            return `
                <div class="chat-card">
                    <div class="chat-card-title">${escapeChatHtml(msg.title || '请求确认')}</div>
                    <div class="chat-card-detail">${formatChatContent(detail)}</div>
                    ${buttons ? `<div class="chat-card-options">${buttons}</div>` : ''}
                    ${formatChatTimestamp(msg) ? `<div class="chat-card-time">${formatChatTimestamp(msg)}</div>` : ''}
                </div>
            `;
        }

        function bindChatCardActions() {
            chatMessages.querySelectorAll('.chat-card-option:not([disabled])').forEach(btn => {
                btn.onclick = () => submitChatCardChoice(
                    btn.getAttribute('data-card-name') || '',
                    btn.getAttribute('data-card-choice') || ''
                );
            });
        }

        function bindChatMessageActions() {
            chatMessages.querySelectorAll('.chat-message-copy-btn').forEach(btn => {
                btn.onclick = async () => {
                    const text = btn.getAttribute('data-chat-copy-text') || '';
                    try {
                        const copied = await copyChatText(text);
                        flashCopyButton(btn, copied);
                    } catch (err) {
                        flashCopyButton(btn, false);
                    }
                };
            });
        }

        function updateChatComposerState() {
            chatSendBtn.disabled = false;
            chatInput.disabled = false;
            chatAttachBtn.disabled = false;
            chatAttachmentInput.disabled = false;
            if (chatAttachmentFolderInput) chatAttachmentFolderInput.disabled = false;
        }

        function setChatStatusDot(state) {
            const hasState = String(state || '').trim().length > 0;
            chatStatusDot.classList.toggle('chat-status-dot-waiting', hasState);
            chatStatusDot.classList.toggle('chat-status-dot-online', !hasState);
            chatStatusDot.title = hasState ? String(state).trim() : '在线';
            chatStatusText.textContent = hasState ? String(state).trim() : '在线';
            chatStatusText.classList.toggle('chat-status-text-waiting', hasState);
        }

        function renderChatAttachmentDrafts() {
            if (!chatAttachmentDrafts) return;
            if (chatPendingFiles.length === 0) {
                chatAttachmentDrafts.innerHTML = '';
                return;
            }
            chatAttachmentDrafts.innerHTML = chatPendingFiles.map((file, idx) => `
                <div class="chat-attachment-draft">
                    <i class="fas fa-paperclip"></i>
                    <span title="${escapeChatHtml(file.name)}">${escapeChatHtml(file.name)}</span>
                    <small>${formatFileSize(file.size)}</small>
                    <button type="button" data-chat-attachment-remove="${idx}" title="移除附件" aria-label="移除附件">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            `).join('');
            chatAttachmentDrafts.querySelectorAll('[data-chat-attachment-remove]').forEach(btn => {
                btn.onclick = () => {
                    const idx = Number(btn.getAttribute('data-chat-attachment-remove'));
                    chatPendingFiles.splice(idx, 1);
                    renderChatAttachmentDrafts();
                };
            });
        }

        async function uploadChatAttachments(files) {
            if (!files.length) return [];
            const formData = new FormData();
            files.forEach(file => formData.append('attachments', file));
            const response = await fetch('/api/chatbot/attachments', {
                method: 'POST',
                headers: authHeaders(),
                body: formData
            });
            if (!response.ok) {
                if (response.status === 401) {
                    clearAuthAndReturn();
                    throw new Error('登录状态失效，请重新登录');
                }
                let errorText = '附件上传失败';
                try {
                    const errorData = await response.json();
                    errorText = errorData.detail || errorText;
                } catch (_) {
                    errorText = await response.text() || errorText;
                }
                throw new Error(errorText);
            }
            const data = await response.json();
            return Array.isArray(data.attachments) ? data.attachments : [];
        }

        async function loadChatHistory() {
            if (chatHistoryLoadPromise) return chatHistoryLoadPromise;
            chatHistoryLoadPromise = (async () => {
                if (!chatHistoryLoaded) {
                    chatHistoryLoaded = false;
                    renderChatMessages();
                }
                try {
                    const response = await fetch('/api/chatbot/history', { headers: authHeaders() });
                    if (!response.ok) {
                        if (response.status === 401) {
                            clearAuthAndReturn();
                            return;
                        }
                        throw new Error(await response.text() || '历史记录加载失败');
                    }
                    const data = await response.json();
                    setChatMessages(Array.isArray(data.messages) ? data.messages : []);
                    if (data.last_update) lastChatUpdate = data.last_update;
                } catch (err) {
                    chatMessages.innerHTML = `<div class="chat-empty-state">聊天记录加载失败：${escapeChatHtml(err.message)}</div>`;
                    chatHistoryLoaded = true;
                } finally {
                    chatHistoryLoadPromise = null;
                }
            })();
            return chatHistoryLoadPromise;
        }

        async function refreshChatStatus() {
            try {
                const response = await fetch('/api/chatbot/status', { headers: authHeaders() });
                if (response.status === 401) {
                    clearAuthAndReturn();
                    return;
                }
                if (response.status === 429) return;
                if (!response.ok) return;
                const data = await response.json();
                setChatStatusDot(data.state || '');
                const nextUpdate = data.last_update || '';
                if (nextUpdate && nextUpdate !== lastChatUpdate) {
                    await loadChatHistory();
                }
            } catch (_) {}
        }

        function ensureChatPolling() {
            if (chatPollTimer) return;
            chatPollTimer = setInterval(refreshChatStatus, 3000);
        }

        async function submitChatCardChoice(name, choice) {
            if (!name || !choice) return;
            const token = typeof window.getAuthToken === 'function'
                ? window.getAuthToken()
                : (sessionStorage.getItem('authToken') || '');
            try {
                const response = await fetch('/api/chatbot', {
                    method: 'POST',
                    headers: { ...authHeaders(), 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name,
                        choice,
                        detail: token
                    })
                });
                if (response.status === 401) {
                    clearAuthAndReturn();
                    return;
                }
                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(errorData.detail || '提交授权选择失败');
                }
                await loadChatHistory();
                await refreshChatStatus();
            } catch (err) {
                setChatStatusDot('在线');
            }
        }

        async function sendChatMessage() {
            const content = chatInput.value.trim();
            const filesToUpload = [...chatPendingFiles];
            if (!content && filesToUpload.length === 0) return;

            chatInput.value = '';
            chatInput.style.height = '';
            await loadChatHistory().catch(() => {});

            let userMessage = null;
            try {
                const uploadedAttachments = await uploadChatAttachments(filesToUpload);
                chatPendingFiles = [];
                renderChatAttachmentDrafts();

                userMessage = {
                    role: 'user',
                    content: content || '请查看本次上传的附件。',
                    attachments: uploadedAttachments,
                    datetime: new Date().toISOString()
                };
                chatMessagesCache.push(userMessage);
                updateLastChatUpdate(chatMessagesCache);
                renderChatMessages();

                const response = await fetch('/api/chatbot', {
                    method: 'POST',
                    headers: { ...authHeaders(), 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        content,
                        attachmentIds: uploadedAttachments.map(item => item.attachmentId)
                    })
                });

                if (!response.ok) {
                    if (response.status === 401) {
                        clearAuthAndReturn();
                        throw new Error('登录状态失效，请重新登录');
                    }
                    let errorText = '请求失败';
                    try {
                        const errorData = await response.json();
                        errorText = errorData.detail || errorText;
                    } catch (_) {
                        errorText = await response.text() || errorText;
                    }
                    throw new Error(errorText);
                }

                await refreshChatStatus();
            } catch (err) {
                const assistantMessage = { role: 'assistant', content: '', datetime: new Date().toISOString() };
                chatMessagesCache.push(assistantMessage);
                assistantMessage.content = `对话失败：${err.message}`;
            } finally {
                renderChatMessages();
            }
        }

        async function exportChatHistory() {
            try {
                const response = await fetch('/api/chatbot/history', { headers: authHeaders() });
                if (!response.ok) {
                    if (response.status === 401) {
                        clearAuthAndReturn();
                        return;
                    }
                    throw new Error(await response.text() || '历史记录加载失败');
                }
                const data = await response.json();
                const messages = Array.isArray(data.messages) ? data.messages : [];
                
                function formatTime(isoStr) {
                    if (!isoStr) return '';
                    const date = new Date(isoStr);
                    if (isNaN(date.getTime())) return isoStr;
                    return date.toLocaleString('zh-CN', { hour12: false });
                }

                let mdText = `# 客服会话记录\n\n`;
                mdText += `* **导出时间**: ${formatTime(new Date().toISOString())}\n`;
                if (data.last_update) {
                    mdText += `* **最后更新**: ${formatTime(data.last_update)}\n`;
                }
                mdText += `\n---\n\n`;

                messages.forEach(msg => {
                    const role = msg.role;
                    const content = msg.content || '';
                    const timeStr = msg.datetime ? ` *(${formatTime(msg.datetime)})*` : '';

                    if (role === 'user') {
                        mdText += `### 👤 我${timeStr}\n\n> ${content.replace(/\n/g, '\n> ')}\n\n`;
                        if (Array.isArray(msg.attachments) && msg.attachments.length > 0) {
                            mdText += `**附件列表**:\n`;
                            msg.attachments.forEach(att => {
                                mdText += `- 📎 [${att.originalName}](${att.relativePath || '#'}) (${(att.size / 1024).toFixed(1)} KB)\n`;
                            });
                            mdText += `\n`;
                        }
                    } else if (role === 'assistant') {
                        if (!content.trim()) return;
                        mdText += `### 🤖 AI 客服${timeStr}\n\n> ${content.replace(/\n/g, '\n> ')}\n\n`;
                        if (Array.isArray(msg.attachments) && msg.attachments.length > 0) {
                            mdText += `**附件列表**:\n`;
                            msg.attachments.forEach(att => {
                                mdText += `- 📎 [${att.originalName}](${att.relativePath || '#'}) (${(att.size / 1024).toFixed(1)} KB)\n`;
                            });
                            mdText += `\n`;
                        }
                    } else if (role === 'notice') {
                        mdText += `> 📢 **系统通知**${timeStr}\n> ${content.replace(/\n/g, '\n> ')}\n\n`;
                    } else if (role === 'card') {
                        mdText += `> 💳 **交互操作卡片: ${msg.title || '请求确认'}**${timeStr}\n`;
                        mdText += `> - **操作名称**: ${msg.name || ''}\n`;
                        if (msg.detail) {
                            mdText += `> - **详情**: ${msg.detail}\n`;
                        }
                        if (Array.isArray(msg.options) && msg.options.length > 0) {
                            mdText += `> - **可选项**: ${msg.options.join(' / ')}\n`;
                        }
                        if (msg.choice) {
                            mdText += `> - **已选结果**: **${msg.choice}**\n`;
                        }
                        mdText += `\n`;
                    }
                    mdText += `\n---\n\n`;
                });

                const blob = new Blob([mdText], { type: 'text/markdown;charset=utf-8' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `chat_history_${new Date().toISOString().slice(0, 10)}.md`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            } catch (err) {
                alert(`导出失败: ${err.message}`);
            }
        }

        // Binding Events
        chatFab.onclick = () => setChatOpen(!chatPanel.classList.contains('open'));
        chatCloseBtn.onclick = () => setChatOpen(false);
        if (chatExportBtn) {
            chatExportBtn.onclick = () => exportChatHistory();
        }
        chatSendBtn.onclick = () => sendChatMessage();
        chatInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                sendChatMessage();
            }
        });
        chatInput.addEventListener('input', () => {
            chatInput.style.height = 'auto';
            chatInput.style.height = chatInput.scrollHeight + 'px';
        });

        chatAttachBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            const isDisp = chatAttachMenu.style.display === 'block';
            chatAttachMenu.style.display = isDisp ? 'none' : 'block';
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (chatAttachMenu && !e.target.closest('.chat-attach-dropdown')) {
                chatAttachMenu.style.display = 'none';
            }
        });

        chatUploadFileOption.addEventListener('click', () => {
            chatAttachMenu.style.display = 'none';
            chatAttachmentInput.click();
        });

        chatUploadFolderOption.addEventListener('click', () => {
            chatAttachMenu.style.display = 'none';
            if (typeof JSZip === 'undefined') {
                alert('压缩组件正在加载中，请稍候重试...');
                return;
            }
            chatAttachmentFolderInput.click();
        });

        chatAttachmentInput.addEventListener('change', () => {
            chatPendingFiles = chatPendingFiles.concat(Array.from(chatAttachmentInput.files || []));
            chatAttachmentInput.value = '';
            renderChatAttachmentDrafts();
        });

        chatAttachmentFolderInput.addEventListener('change', async () => {
            const files = chatAttachmentFolderInput.files;
            if (!files || files.length === 0) return;

            const folderFiles = Array.from(files);
            const topDirName = folderFiles[0].webkitRelativePath.split('/')[0] || 'folder';

            chatAttachBtn.disabled = true;
            chatSendBtn.disabled = true;

            try {
                const zip = new JSZip();
                folderFiles.forEach(f => {
                    const relPath = f.webkitRelativePath || f.name;
                    zip.file(relPath, f);
                });

                const zipBlob = await zip.generateAsync({
                    type: 'blob',
                    compression: 'DEFLATE',
                    compressionOptions: { level: 6 }
                });

                const zipFile = new File([zipBlob], `${topDirName}.zip`, { type: 'application/zip' });
                chatPendingFiles.push(zipFile);
                renderChatAttachmentDrafts();
            } catch (err) {
                console.error(err);
                alert('文件夹压缩打包失败: ' + err.message);
            } finally {
                chatAttachBtn.disabled = false;
                chatSendBtn.disabled = false;
                chatAttachmentFolderInput.value = '';
            }
        });

        // Initialize Chat History load
        loadChatHistory().catch((err) => {
            console.error('Initial chatbot history load failed', err);
        });
        ensureChatPolling();
        updateChatComposerState();
    });
})();
