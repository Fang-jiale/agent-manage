const { contextBridge, ipcRenderer } = require('electron');
contextBridge.exposeInMainWorld('ywmDesktop', {
  request: (path, options) => ipcRenderer.invoke('client:request', path, options),
  chooseDirectory: () => ipcRenderer.invoke('desktop:directory'),
  openExternal: url => ipcRenderer.invoke('desktop:external', url),
  settings: options => ipcRenderer.invoke('desktop:settings', options),
  diagnostics: () => ipcRenderer.invoke('desktop:diagnostics'),
  restartCore: () => ipcRenderer.invoke('desktop:restart'),
});
