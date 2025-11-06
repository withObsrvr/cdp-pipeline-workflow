# CDP Pipeline Workflow - Documentation

This directory contains the GitHub Pages landing page for CDP Pipeline Workflow.

## Contents

- **index.html** - Main installation landing page (rustup.rs style)
- **_config.yml** - GitHub Pages configuration
- **.nojekyll** - Prevents Jekyll processing (we use custom HTML)
- **SETUP_GITHUB_PAGES.md** - Guide for enabling GitHub Pages

## Live Site

Once GitHub Pages is enabled, the landing page will be available at:

**https://withobsrvr.github.io/cdp-pipeline-workflow/**

## Local Development

To preview the landing page locally:

```bash
# Simple HTTP server (Python)
cd docs/
python3 -m http.server 8000

# Or use Node.js
npx serve

# Or use PHP
php -S localhost:8000
```

Then visit: http://localhost:8000

## Features

The landing page provides:

- **Three Installation Options:**
  - Docker (recommended)
  - Pre-built binaries (Linux, macOS)
  - Build from source

- **Copy-to-Clipboard:** One-click command copying
- **Responsive Design:** Works on mobile and desktop
- **Minimal Dependencies:** Pure HTML/CSS/JS (no frameworks)

## Updating

To update the landing page:

1. Edit `index.html`
2. Test locally (optional)
3. Commit and push:
   ```bash
   git add docs/index.html
   git commit -m "Update landing page"
   git push
   ```

GitHub Pages will automatically rebuild in 1-3 minutes.

## Customization

### Colors

The gradient background uses these colors:
- Primary: `#667eea` (purple)
- Secondary: `#764ba2` (darker purple)

To change colors, edit the CSS in `index.html`:

```css
body {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}
```

### Tabs

To add/remove installation tabs, edit the HTML structure:

```html
<div class="tabs">
    <div class="tab active" onclick="switchTab('docker')">Docker</div>
    <!-- Add more tabs here -->
</div>

<div id="docker" class="tab-content active">
    <!-- Tab content here -->
</div>
```

### Links

Update links in the footer section:

```html
<a href="https://github.com/withObsrvr/cdp-pipeline-workflow" class="link-button">
    View on GitHub
</a>
```

## Browser Compatibility

Tested and working on:
- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- Mobile browsers (iOS Safari, Chrome Mobile)

## License

Same as the main project license.
