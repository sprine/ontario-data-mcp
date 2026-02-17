/**
 * State-machine SQL syntax highlighter.
 * Produces HTML with <span> classes: kw, fn, str, num, comment
 */
(function (root) {
  'use strict';

  var KEYWORDS = new Set([
    'SELECT','FROM','WHERE','JOIN','LEFT','RIGHT','INNER','OUTER','CROSS',
    'ON','AND','OR','NOT','IN','AS','IS','NULL','BETWEEN','LIKE','GROUP',
    'BY','ORDER','HAVING','LIMIT','OFFSET','UNION','ALL','DISTINCT','CASE',
    'WHEN','THEN','ELSE','END','INSERT','UPDATE','DELETE','CREATE','DROP',
    'ALTER','TABLE','INDEX','VIEW','WITH','DESC','ASC','PARTITION','OVER',
    'EXISTS','CAST','INTO','SET','VALUES','PRIMARY','KEY','FOREIGN',
    'REFERENCES','DEFAULT','CHECK','CONSTRAINT','IF','REPLACE','TEMPORARY',
    'TEMP','RECURSIVE','MATERIALIZED','LATERAL','NATURAL','USING','FETCH',
    'FIRST','NEXT','ROWS','ONLY','FILTER','WITHIN','RANGE','UNBOUNDED',
    'PRECEDING','FOLLOWING','CURRENT','ROW'
  ]);

  var FUNCTIONS = new Set([
    'AVG','SUM','COUNT','MIN','MAX','ROUND','LAG','LEAD','FIRST','LAST',
    'RANK','ROW_NUMBER','DENSE_RANK','COALESCE','NULLIF','TRY_CAST',
    'YEAR','MONTH','DAY','LEFT','RIGHT','LENGTH','UPPER','LOWER','TRIM',
    'CONCAT','ST_CONTAINS','ST_POINT','ST_DISTANCE','EPOCH','FLOOR',
    'CEIL','ABS','SUBSTR','SUBSTRING','REPLACE','CAST','DATE_TRUNC',
    'EXTRACT','NOW','DATE_PART','STRING_AGG','ARRAY_AGG','NTILE',
    'PERCENT_RANK','CUME_DIST','NTH_VALUE','FIRST_VALUE','LAST_VALUE'
  ]);

  function esc(ch) {
    switch (ch) {
      case '&': return '&amp;';
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '"': return '&quot;';
      default:  return ch;
    }
  }

  function escStr(s) {
    return s.replace(/[&<>"]/g, esc);
  }

  function highlightSQL(sql) {
    var out = '';
    var i = 0;
    var len = sql.length;

    while (i < len) {
      var ch = sql[i];

      // -- line comment
      if (ch === '-' && i + 1 < len && sql[i + 1] === '-') {
        var end = sql.indexOf('\n', i);
        if (end === -1) end = len;
        out += '<span class="comment">' + escStr(sql.slice(i, end)) + '</span>';
        i = end;
        continue;
      }

      // string literal
      if (ch === "'") {
        var j = i + 1;
        while (j < len) {
          if (sql[j] === "'" && j + 1 < len && sql[j + 1] === "'") {
            j += 2; // escaped quote
          } else if (sql[j] === "'") {
            j++;
            break;
          } else {
            j++;
          }
        }
        out += '<span class="str">' + escStr(sql.slice(i, j)) + '</span>';
        i = j;
        continue;
      }

      // number (digit or leading dot followed by digit)
      if (
        (ch >= '0' && ch <= '9') ||
        (ch === '.' && i + 1 < len && sql[i + 1] >= '0' && sql[i + 1] <= '9')
      ) {
        var j = i;
        var hasDot = false;
        while (j < len) {
          var c = sql[j];
          if (c >= '0' && c <= '9') { j++; }
          else if (c === '.' && !hasDot) { hasDot = true; j++; }
          else { break; }
        }
        out += '<span class="num">' + escStr(sql.slice(i, j)) + '</span>';
        i = j;
        continue;
      }

      // word (identifier, keyword, or function)
      if (
        (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch === '_'
      ) {
        var j = i + 1;
        while (j < len) {
          var c = sql[j];
          if (
            (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c === '_'
          ) {
            j++;
          } else {
            break;
          }
        }
        var word = sql.slice(i, j);
        var upper = word.toUpperCase();
        if (KEYWORDS.has(upper)) {
          out += '<span class="kw">' + escStr(word) + '</span>';
        } else if (FUNCTIONS.has(upper)) {
          out += '<span class="fn">' + escStr(word) + '</span>';
        } else {
          out += escStr(word);
        }
        i = j;
        continue;
      }

      // everything else (operators, parens, whitespace, etc.)
      out += esc(ch);
      i++;
    }

    return out;
  }

  root.highlightSQL = highlightSQL;
})(this);
