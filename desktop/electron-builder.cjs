module.exports = {
  appId: 'com.ywmatrix.client', productName: 'YwMatrix',
  directories: { output: '../dist/desktop' },
  files: ['main.cjs', 'preload.cjs', 'bridge.cjs', 'package.json'],
  extraResources: [{ from: 'resources/core', to: 'core' }],
  asar: true,
  win: { target: [{ target: 'nsis', arch: ['x64'] }], icon: '../static/icon-512.png', signExecutable: false },
  nsis: { oneClick: false, perMachine: false, allowToChangeInstallationDirectory: true, createDesktopShortcut: true,
    deleteAppDataOnUninstall: false, artifactName: 'YwMatrix-Setup-${version}-${arch}.exe' },
  linux: { target: [{ target: 'tar.gz', arch: ['arm64'] }], category: 'Utility', executableName: 'ywmatrix', icon: '../static/icon-512.png',
    artifactName: 'YwMatrix-${version}-linux-${arch}-portable.tar.gz' },
  mac: { target: ['dmg'], category: 'public.app-category.productivity' },
};
