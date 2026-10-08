// Drafts belong to an account, agent and session; IndexedDB also keeps attachments
// without competing with the small localStorage quota used for chat history.
export function draftKey(userId, agentId, sessionId) {
    return userId && agentId && sessionId ? JSON.stringify([userId, agentId, sessionId]) : null;
}

export function isCompositionKey(event) {
    return event.isComposing || event.keyCode === 229;
}

export class DraftStore {
    constructor(persistence, onError = () => {}) {
        this.persistence = persistence;
        this.onError = onError;
        this.cache = new Map();
        this.queue = Promise.resolve();
    }

    async read(key) {
        if (!this.cache.has(key)) {
            let value;
            try { value = await this.persistence.read(key); }
            catch (error) { this.onError(error); }
            // A write/delete while the read was pending always wins.
            if (!this.cache.has(key)) this.cache.set(key, value || { text: '', files: [] });
        }
        const value = this.cache.get(key);
        return { text: value.text, files: [...value.files] };
    }

    write(key, value) {
        if (!key) return;
        const snapshot = { text: value.text, files: [...value.files] };
        this.cache.set(key, snapshot);
        this.queue = this.queue.then(() => this.persistence.write(key, snapshot)).catch(this.onError);
    }

    delete(key) {
        if (!key) return;
        this.cache.set(key, { text: '', files: [] });
        this.queue = this.queue.then(() => this.persistence.delete(key)).catch(this.onError);
    }
}

export function indexedDraftPersistence(indexedDB) {
    let opening;
    function database() {
        if (!opening) opening = new Promise((resolve, reject) => {
            if (!indexedDB) { reject(new Error('Draft storage unavailable')); return; }
            const request = indexedDB.open('ywm-composer', 1);
            request.onupgradeneeded = () => request.result.createObjectStore('drafts');
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
            request.onblocked = () => reject(new Error('Draft storage blocked'));
        });
        return opening;
    }
    async function transact(mode, action) {
        const db = await database();
        return new Promise((resolve, reject) => {
            const tx = db.transaction('drafts', mode);
            const request = action(tx.objectStore('drafts'));
            tx.oncomplete = () => resolve(request.result);
            tx.onerror = () => reject(tx.error);
            tx.onabort = () => reject(tx.error || new Error('Draft storage aborted'));
        });
    }
    return {
        read: key => transact('readonly', store => store.get(key)),
        write: (key, value) => transact('readwrite', store => store.put(value, key)),
        delete: key => transact('readwrite', store => store.delete(key))
    };
}
