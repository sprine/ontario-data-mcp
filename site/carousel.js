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

  function initNav(track) {
    var section = track.closest('.carousel-section');
    var cards = track.querySelectorAll('.carousel-card');
    var nav = section ? section.querySelector('.carousel-nav') : null;
    if (!nav || cards.length < 2) {
      if (nav) nav.style.display = 'none';
      return;
    }

    var dotsContainer = nav.querySelector('.carousel-dots');
    var prevBtn = nav.querySelector('.carousel-prev');
    var nextBtn = nav.querySelector('.carousel-next');

    for (var i = 0; i < cards.length; i++) {
      (function (idx) {
        var dot = document.createElement('button');
        dot.className = 'carousel-dot' + (idx === 0 ? ' active' : '');
        dot.setAttribute('aria-label', 'Go to example ' + (idx + 1));
        dot.addEventListener('click', function () { scrollToCard(idx); });
        dotsContainer.appendChild(dot);
      })(i);
    }

    var dots = dotsContainer.querySelectorAll('.carousel-dot');

    function scrollToCard(index) {
      var card = cards[index];
      if (!card) return;
      var cardCenter = card.offsetLeft + card.offsetWidth / 2;
      var trackCenter = track.clientWidth / 2;
      track.scrollTo({ left: cardCenter - trackCenter, behavior: 'smooth' });
    }

    function getActiveIndex() {
      var center = track.scrollLeft + track.clientWidth / 2;
      var closest = 0;
      var closestDist = Infinity;
      for (var i = 0; i < cards.length; i++) {
        var cardCenter = cards[i].offsetLeft + cards[i].offsetWidth / 2;
        var dist = Math.abs(cardCenter - center);
        if (dist < closestDist) {
          closestDist = dist;
          closest = i;
        }
      }
      return closest;
    }

    function updateDots() {
      var active = getActiveIndex();
      for (var i = 0; i < dots.length; i++) {
        dots[i].classList.toggle('active', i === active);
      }
      prevBtn.disabled = active === 0;
      nextBtn.disabled = active === cards.length - 1;
    }

    prevBtn.addEventListener('click', function () {
      var active = getActiveIndex();
      if (active > 0) scrollToCard(active - 1);
    });

    nextBtn.addEventListener('click', function () {
      var active = getActiveIndex();
      if (active < cards.length - 1) scrollToCard(active + 1);
    });

    var scrollTimer;
    track.addEventListener('scroll', function () {
      clearTimeout(scrollTimer);
      scrollTimer = setTimeout(updateDots, 50);
    }, { passive: true });

    updateDots();
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

    initNav(track);
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
