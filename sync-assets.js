const fs = require('fs');
const path = require('path');

const css = fs.readFileSync(path.join(__dirname, 'style.css'), 'utf-8');
let html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf-8');

if (!html.includes('id="embedded-msk-theme"')) {
  const replacement = '<link rel="stylesheet" href="style.css">\n  <style id="embedded-msk-theme">\n' + css + '\n  </style>';
  html = html.replace('<link rel="stylesheet" href="style.css">', replacement);
  fs.writeFileSync(path.join(__dirname, 'index.html'), html, 'utf-8');
  console.log('Injected style into index.html');
}

const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) {
  fs.mkdirSync(publicDir, { recursive: true });
}

fs.copyFileSync(path.join(__dirname, 'index.html'), path.join(publicDir, 'index.html'));
fs.copyFileSync(path.join(__dirname, 'style.css'), path.join(publicDir, 'style.css'));
fs.copyFileSync(path.join(__dirname, 'app.js'), path.join(publicDir, 'app.js'));

console.log('Static assets synced to public/ directory!');
