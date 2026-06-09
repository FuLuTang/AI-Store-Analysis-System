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
                        <h3><i class="fas fa-comments"></i> 对话助手 <span class="chat-status-dot" title="在线"></span></h3>
                        <div class="chat-run-badge">账号级聊天记录</div>
                    </div>
                    <button id="chatCloseBtn" class="chat-panel-close" type="button" aria-label="关闭对话助手">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
                <div id="chatMessages" class="chat-messages">
                    <div class="chat-empty-state">从这里直接开始聊天。</div>
                </div>
                <div class="chat-composer">
                    <textarea id="chatInput" class="chat-input" placeholder="输入你想问的问题..." rows="3"></textarea>
                    <input type="file" id="chatAttachmentInput" hidden multiple>
                    <div id="chatAttachmentDrafts" class="chat-attachment-drafts"></div>
                    <div class="chat-composer-actions">
                        <span id="chatStatusHint" class="chat-status-hint">账号级多轮对话</span>
                        <div class="chat-action-buttons">
                            <button id="chatAttachBtn" class="chat-icon-btn" type="button" title="添加附件" aria-label="添加附件">
                                <i class="fas fa-paperclip"></i>
                            </button>
                            <button id="chatSendBtn" class="btn" type="button">
                                <i class="fas fa-paper-plane"></i> 发送
                            </button>
                        </div>
                    </div>
                </div>
            </section>
        `;
        document.body.appendChild(chatContainer);

        // DOM elements select
        const chatFab = document.getElementById('chatFab');
        const chatPanel = document.getElementById('chatPanel');
        const chatCloseBtn = document.getElementById('chatCloseBtn');
        const chatMessages = document.getElementById('chatMessages');
        const chatInput = document.getElementById('chatInput');
        const chatAttachmentInput = document.getElementById('chatAttachmentInput');
        const chatAttachmentDrafts = document.getElementById('chatAttachmentDrafts');
        const chatAttachBtn = document.getElementById('chatAttachBtn');
        const chatSendBtn = document.getElementById('chatSendBtn');
        const chatStatusHint = document.getElementById('chatStatusHint');

        // Chatbot state
        let chatMessagesCache = [];
        let chatHistoryLoaded = false;
        let chatHistoryLoadPromise = null;
        let chatPendingFiles = [];

        onMarkedLoaded = () => {
            if (chatHistoryLoaded) {
                renderChatMessages();
            }
        };

        // Auth helper
        function authHeaders() {
            const key = sessionStorage.getItem('authToken') || '';
            return { 'X-Auth-Token': key };
        }

        function clearAuthAndReturn() {
            sessionStorage.removeItem('authToken');
            sessionStorage.removeItem('accountName');
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
            if (typeof marked !== 'undefined' && marked && typeof marked.parse === 'function') {
                return marked.parse(safeText);
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

        function normalizeChatRecord(record) {
            if (!record || typeof record !== 'object') return null;
            if (!record.role) return null;
            return { ...record };
        }

        function formatChatTimestamp(record) {
            if (!record || !record.datetime) return '';
            const dt = new Date(record.datetime);
            if (Number.isNaN(dt.getTime())) return record.datetime;
            return dt.toLocaleString('zh-CN', { hour12: false });
        }

        function setChatMessages(messages) {
            chatMessagesCache = (messages || []).map(normalizeChatRecord).filter(Boolean);
            chatHistoryLoaded = true;
            renderChatMessages();
        }

        function setChatOpen(open) {
            chatPanel.classList.toggle('open', open);
            chatPanel.setAttribute('aria-hidden', open ? 'false' : 'true');
            chatFab.setAttribute('aria-expanded', open ? 'true' : 'false');
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
                if (msg.role === 'user') return true;
                if (msg.role !== 'assistant') return false;
                return String(msg.content || '').trim().length > 0;
            });
            if (history.length === 0) {
                chatMessages.innerHTML = '<div class="chat-empty-state">从这里直接开始聊天。</div>';
                return;
            }

            chatMessages.innerHTML = history.map(msg => `
                ${msg.role === 'notice' ? `
                    <div class="chat-notice">
                        <span class="markdown-body chat-markdown">${formatChatContent(msg.content)}</span>
                    </div>
                ` : `
                <div class="chat-message ${msg.role === 'user' ? 'chat-message-user' : 'chat-message-assistant'}">
                    <div class="chat-message-role">
                        ${msg.role === 'user' ? '你' : 'AI'}
                        ${formatChatTimestamp(msg) ? ` · ${formatChatTimestamp(msg)}` : ''}
                    </div>
                    <div class="chat-message-bubble">
                        <div class="markdown-body chat-markdown">${formatChatContent(msg.content)}</div>
                        ${renderChatAttachments(msg.attachments)}
                    </div>
                </div>
                `}
            `).join('');
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function updateChatComposerState() {
            chatSendBtn.disabled = false;
            chatInput.disabled = false;
            chatAttachBtn.disabled = false;
            chatAttachmentInput.disabled = false;
            chatStatusHint.textContent = '账号级多轮对话';
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
                chatHistoryLoaded = false;
                renderChatMessages();
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
                } catch (err) {
                    chatMessages.innerHTML = `<div class="chat-empty-state">聊天记录加载失败：${escapeChatHtml(err.message)}</div>`;
                    chatHistoryLoaded = true;
                } finally {
                    chatHistoryLoadPromise = null;
                }
            })();
            return chatHistoryLoadPromise;
        }

        async function sendChatMessage() {
            const content = chatInput.value.trim();
            const filesToUpload = [...chatPendingFiles];
            if (!content && filesToUpload.length === 0) return;

            chatInput.value = '';
            await loadChatHistory().catch(() => {});

            let userMessage = null;
            let assistantMessage = null;

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
                assistantMessage = { role: 'assistant', content: '', datetime: new Date().toISOString() };
                chatMessagesCache.push(userMessage);
                chatMessagesCache.push(assistantMessage);
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

                const reader = response.body.getReader();
                const decoder = new TextDecoder('utf-8');
                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    assistantMessage.content += decoder.decode(value, { stream: true });
                    renderChatMessages();
                }
                assistantMessage.content += decoder.decode();
                if (!assistantMessage.content.trim()) {
                    assistantMessage.content = '当前没有返回可显示的内容。';
                }
                await loadChatHistory().catch(() => {});
            } catch (err) {
                if (!assistantMessage) {
                    assistantMessage = { role: 'assistant', content: '', datetime: new Date().toISOString() };
                    chatMessagesCache.push(assistantMessage);
                }
                assistantMessage.content = `对话失败：${err.message}`;
            } finally {
                renderChatMessages();
            }
        }

        // Binding Events
        chatFab.onclick = () => setChatOpen(!chatPanel.classList.contains('open'));
        chatCloseBtn.onclick = () => setChatOpen(false);
        chatSendBtn.onclick = () => sendChatMessage();
        chatInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                sendChatMessage();
            }
        });

        chatAttachBtn.addEventListener('click', () => chatAttachmentInput.click());
        chatAttachmentInput.addEventListener('change', () => {
            chatPendingFiles = chatPendingFiles.concat(Array.from(chatAttachmentInput.files || []));
            chatAttachmentInput.value = '';
            renderChatAttachmentDrafts();
        });

        // Initialize Chat History load
        loadChatHistory().catch((err) => {
            console.error('Initial chatbot history load failed', err);
        });
        updateChatComposerState();
    });
})();
