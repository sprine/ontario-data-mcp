/**
 * Carousel renderer: fetches examples.json, builds example cards.
 * Expects a .carousel-track container to inject into.
 */
(function () {
  'use strict';

  function esc(s) {
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function renderSources(sources) {
    if (!sources || !sources.length) return '';
    var prefix = sources.length === 1 ? 'Source: ' : 'Sources: ';
    var links = sources.map(function (s) {
      return '<a href="' + esc(s.url) + '" target="_blank" rel="noopener">' +
        esc(s.title) + '</a>';
    });
    var org = sources[0].org ? ' &mdash; ' + esc(sources[0].org) : '';
    return prefix + links.join(', ') + org;
  }

  function renderSteps(steps) {
    return steps.map(function (s) {
      var desc = s.description ? ' &rarr; ' + esc(s.description) : '';
      return '<div class="hood-step"><span class="tool-name">' +
        esc(s.tool) + '</span>' + desc + '</div>';
    }).join('');
  }

  function renderHood(hood) {
    if (!hood) return '';
    var html = '<details><summary>Under the hood</summary><div class="hood-content">';
    html += renderSteps(hood.steps);
    if (hood.sql) {
      html += '<div class="hood-code">' + highlightSQL(hood.sql) + '</div>';
    }
    html += '</div></details>';
    return html;
  }

  function renderFollowup(fu) {
    if (!fu) return '';
    var html = '<details><summary>Follow-up: ' + esc(fu.question) + '</summary>';
    html += '<div class="followup-content">';
    html += '<div class="carousel-a">' + esc(fu.answer);
    if (fu.punchline) html += ' <strong>' + esc(fu.punchline) + '</strong>';
    html += '</div>';
    if (fu.sources && fu.sources.length) {
      html += '<div class="carousel-sources">' + renderSources(fu.sources) + '</div>';
    }
    html += '</div></details>';
    return html;
  }

  function renderCard(ex) {
    var html = '<div class="carousel-card" role="listitem" aria-label="' +
      esc(ex.tag) + ': ' + esc(ex.question) + '">';
    html += '<div class="carousel-tag">' + esc(ex.tag) + '</div>';
    html += '<div class="carousel-q">' + esc(ex.question) + '</div>';
    html += '<div class="carousel-a">' + esc(ex.answer) +
      ' <strong>' + esc(ex.punchline) + '</strong></div>';
    html += '<div class="carousel-sources">' + renderSources(ex.sources) + '</div>';
    html += renderHood(ex.hood);
    html += renderFollowup(ex.followup);
    html += '</div>';
    return html;
  }

  function render(examples) {
    var track = document.querySelector('.carousel-track');
    if (!track || !examples || !examples.length) return;

    track.innerHTML = examples.map(renderCard).join('');
    track.setAttribute('role', 'list');
    track.setAttribute('aria-label', 'Example queries');
    track.setAttribute('aria-live', 'polite');

    var section = track.closest('.carousel-section');
    if (section) section.style.display = '';
  }

  function init() {
    fetch('examples.json')
      .then(function (r) { return r.json(); })
      .then(render)
      .catch(function () { /* silently skip if fetch fails */ });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
