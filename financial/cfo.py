"""AI CFO — answers questions about A&N's P&L, grounded in the dashboard data.

Backs the POST /financials/ask route. Loads the same pnl.json the dashboard
serves, pre-computes the headline numbers in code so the model never has to do
shaky arithmetic, and asks Claude to reason like a CFO over the real figures.

Uses the official Anthropic SDK. The key comes from ANTHROPIC_API_KEY
(jobber-app/.env, never committed).
"""
import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-opus-4-8"
PNL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "financial_dashboard", "pnl.json",
)
NET_TARGET = 0.15


def _load():
    with open(PNL_PATH) as f:
        data = json.load(f)
    return data.get("meta", {}), data.get("months", [])


def _money(x):
    return f"${x:,.0f}" if isinstance(x, (int, float)) else "n/a"


def _pct(x):
    return f"{x * 100:.1f}%" if isinstance(x, (int, float)) else "n/a"


def _build_facts(months):
    """Pre-compute the headline numbers so the model cites rather than calculates."""
    complete = [m for m in months if not m.get("incomplete")]
    out = []
    latest = months[-1]
    out.append(
        f"Latest month {latest['label']}: revenue {_money(latest.get('revenue'))}, "
        f"gross margin {_pct(latest.get('grossMargin'))}, "
        f"net profit {_money(latest.get('netProfit'))} ({_pct(latest.get('netMargin'))} net)."
    )
    if len(months) >= 2:
        p = months[-2]
        out.append(f"Prior month {p['label']}: revenue {_money(p.get('revenue'))}, net profit {_money(p.get('netProfit'))}.")
    ly = next((m for m in months if m.get("year") == latest.get("year", 0) - 1
               and m.get("month") == latest.get("month")), None)
    if ly:
        out.append(f"Same month last year {ly['label']}: revenue {_money(ly.get('revenue'))}, net profit {_money(ly.get('netProfit'))}.")

    t12 = months[-12:]
    out.append(
        f"Trailing 12 months through {latest['label']}: revenue {_money(sum(m.get('revenue') or 0 for m in t12))}, "
        f"net profit {_money(sum(m.get('netProfit') or 0 for m in t12))}."
    )
    gms = [m["grossMargin"] for m in complete if isinstance(m.get("grossMargin"), (int, float))]
    nms = [m["netMargin"] for m in complete if isinstance(m.get("netMargin"), (int, float))]
    if gms:
        out.append(f"Average gross margin (complete months): {_pct(sum(gms) / len(gms))}.")
    if nms:
        out.append(f"Average net margin (complete months): {_pct(sum(nms) / len(nms))}.")
    below = [m for m in complete if isinstance(m.get("netMargin"), (int, float)) and m["netMargin"] < NET_TARGET]
    out.append(f"Months below the 15% net target: {len(below)} of {len(complete)} complete months.")

    rev = [m for m in months if isinstance(m.get("revenue"), (int, float))]
    if rev:
        b = max(rev, key=lambda m: m["revenue"])
        out.append(f"Best revenue month: {b['label']} ({_money(b['revenue'])}).")
        sold = [m for m in rev if m["revenue"] > 0]
        if sold:
            w = min(sold, key=lambda m: m["revenue"])
            out.append(f"Lowest revenue month with sales: {w['label']} ({_money(w['revenue'])}).")
    npf = [m for m in months if isinstance(m.get("netProfit"), (int, float))]
    if npf:
        out.append(f"Best net-profit month: {max(npf, key=lambda m: m['netProfit'])['label']}.")
        out.append(f"Worst net-profit month: {min(npf, key=lambda m: m['netProfit'])['label']}.")
    return "\n".join(out)


def _build_table(months):
    header = "month | revenue | COGS | gross profit | gross margin | operating expenses | net profit | net margin | flags"
    rows = [header]
    for m in months:
        flags = "; ".join(m.get("flags") or []) or ("incomplete" if m.get("incomplete") else "")
        rows.append(" | ".join([
            m.get("label", ""), _money(m.get("revenue")), _money(m.get("cogs")),
            _money(m.get("grossProfit")), _pct(m.get("grossMargin")),
            _money(m.get("operatingExpenses")), _money(m.get("netProfit")),
            _pct(m.get("netMargin")), flags,
        ]))
    return "\n".join(rows)


SYSTEM_PERSONA = """You are the CFO for A&N Outdoor Services, a landscaping and hardscape contractor (S-Corp) in the northwest suburbs of Chicago. You report to the two owners: Alex (operations and admin) and Niko (sales and production).

Your job: answer their questions about the company's finances using ONLY the P&L data provided below. Talk like a sharp, plain-spoken CFO who knows the business, not a textbook.

Hard rules:
- Use ONLY the numbers in the data. NEVER invent, estimate, or guess a figure. If the data does not contain it, say "That's not in the P&L" and say what you'd need to answer it.
- Cite the month for every number you give (for example, "May 2026").
- Lead with the answer, then the why. Keep it short and direct. No finance jargon. No em dashes.
- If something looks off or risky, say so plainly.

How to read THESE specific books (important):
- Targets are 45% gross margin and 15% net margin.
- Gross margin reads HIGH here (often 70 to 80%) because field labor and wages sit in Operating Expenses, not in COGS. So the 45% gross target is NOT comparable to these numbers. Do not tell them they are smashing a 45% gross target. Net margin is the target that actually matters.
- "Net profit" means Net Income, the true bottom line (includes vehicle, fuel, fees, and year-end entries).
- December months can swing sharply negative on net profit even when operations were fine, because year-end and below-the-line entries land in December. Call this out if it comes up.
- The books are cash basis. Revenue is QuickBooks "Total Income," already net of discounts.
- There is no maintenance-versus-install revenue split in this data. If asked, say it is not broken out in the P&L and would have to come from Jobber."""


def answer_question(question, history=None):
    """Return {"answer": str} or {"error": str}."""
    try:
        meta, months = _load()
    except Exception as e:
        logger.error(f"CFO: failed to load pnl.json: {e}")
        return {"error": "Could not load the P&L data."}
    if not months:
        return {"error": "No P&L data available yet."}

    data_block = (
        f"DATA FRESHNESS: parsed {meta.get('generatedAt', '?')}, "
        f"basis {meta.get('basis', '?')}, {meta.get('monthsCount', '?')} months.\n\n"
        f"PRE-COMPUTED FACTS (already calculated for you, cite verbatim):\n{_build_facts(months)}\n\n"
        f"FULL MONTH-BY-MONTH TABLE:\n{_build_table(months)}\n\n"
        f"DATA-QUALITY NOTES:\n" + "\n".join(f"- {c}" for c in meta.get("caveats", []))
    )

    messages = []
    for turn in (history or [])[-6:]:
        if turn.get("role") in ("user", "assistant") and turn.get("content"):
            messages.append({"role": turn["role"], "content": str(turn["content"])})
    messages.append({"role": "user", "content": str(question)})

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("CFO: ANTHROPIC_API_KEY not set")
        return {"error": "The AI CFO isn't configured yet (missing API key)."}

    payload = {
        "model": MODEL,
        "max_tokens": 4000,
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": "medium"},
        "system": [
            {"type": "text", "text": SYSTEM_PERSONA},
            {"type": "text", "text": data_block, "cache_control": {"type": "ephemeral"}},
        ],
        "messages": messages,
    }
    try:
        r = requests.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=120,
        )
    except requests.RequestException as e:
        logger.error(f"CFO: request to Anthropic failed: {e}")
        return {"error": "The AI CFO couldn't reach Claude. Try again in a moment."}

    if r.status_code == 401:
        logger.error("CFO: Anthropic returned 401 (bad API key)")
        return {"error": "The AI CFO isn't configured (API key invalid)."}
    if not r.ok:
        logger.error(f"CFO: Anthropic API {r.status_code}: {r.text[:400]}")
        return {"error": "The AI CFO had a problem. Try again in a moment."}

    data = r.json()
    if data.get("stop_reason") == "refusal":
        return {"answer": "I can't answer that one. Try rephrasing as a question about the P&L numbers."}
    text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
    return {"answer": text or "I couldn't form an answer to that."}


# Self-contained chat widget injected into the served dashboard by the /financials
# route. Plain HTML/CSS/JS (no build step, no node_modules) so it ships even when
# the React app can't be rebuilt locally. Posts to /financials/ask. A floating
# button bottom-right opens the panel.
CHAT_WIDGET_HTML = """
<style>
#cfo-fab{position:fixed;right:20px;bottom:20px;z-index:9998;background:#1f5421;color:#fff;border:none;border-radius:9999px;padding:12px 18px;font:600 14px/1 system-ui,-apple-system,sans-serif;box-shadow:0 4px 14px rgba(0,0,0,.2);cursor:pointer}
#cfo-fab:hover{background:#173f19}
#cfo-panel{position:fixed;right:20px;bottom:20px;z-index:9999;width:380px;max-width:calc(100vw - 40px);height:560px;max-height:calc(100vh - 40px);background:#fff;border:1px solid #e5e7eb;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,.25);display:none;flex-direction:column;overflow:hidden;font:14px system-ui,-apple-system,sans-serif}
#cfo-panel.open{display:flex}
#cfo-head{background:#173f19;color:#fff;padding:13px 16px;display:flex;justify-content:space-between;align-items:center}
#cfo-head h3{margin:0;font-size:15px;font-weight:600}
#cfo-head button{background:transparent;border:none;color:#cdebcf;font-size:22px;cursor:pointer;line-height:1;padding:0}
#cfo-msgs{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:10px;background:#f9fafb}
#cfo-intro{font-size:12px;color:#6b7280;margin:0 0 4px}
.cfo-u{align-self:flex-end;background:#1f5421;color:#fff;border-radius:14px 14px 4px 14px;padding:8px 12px;max-width:85%}
.cfo-a{align-self:flex-start;background:#fff;border:1px solid #eee;color:#1f2937;border-radius:14px 14px 14px 4px;padding:8px 12px;max-width:92%;white-space:pre-wrap;line-height:1.5}
.cfo-e{align-self:stretch;background:#fef2f2;border:1px solid #fecaca;color:#b91c1c;border-radius:8px;padding:8px 10px;font-size:13px}
.cfo-sug{text-align:left;width:100%;background:#f0f7f0;border:1px solid #dcebdc;color:#1f5421;border-radius:10px;padding:8px 10px;margin-bottom:6px;cursor:pointer;font:13px system-ui,-apple-system,sans-serif}
.cfo-sug:hover{background:#dcebdc}
#cfo-form{display:flex;gap:8px;padding:12px;border-top:1px solid #eee;background:#fff}
#cfo-in{flex:1;border:1px solid #ddd;border-radius:10px;padding:9px 11px;font:14px system-ui,-apple-system,sans-serif;outline:none}
#cfo-in:focus{border-color:#1f5421}
#cfo-send{background:#1f5421;color:#fff;border:none;border-radius:10px;padding:0 16px;font:600 14px system-ui,-apple-system,sans-serif;cursor:pointer}
#cfo-send:disabled{opacity:.4;cursor:default}
.cfo-load{align-self:flex-start;color:#9ca3af;font-size:13px;padding:2px 6px}
</style>
<button id="cfo-fab" onclick="cfoOpen()">Ask your CFO</button>
<div id="cfo-panel">
  <div id="cfo-head"><h3>Ask your CFO</h3><button onclick="cfoClose()" aria-label="Close">&times;</button></div>
  <div id="cfo-msgs"><p id="cfo-intro">Plain-English answers from your real P&amp;L. Cites the month for every number and won't make figures up.</p></div>
  <form id="cfo-form" onsubmit="return cfoAsk()">
    <input id="cfo-in" placeholder="Ask about revenue, margins, a month..." autocomplete="off"/>
    <button id="cfo-send" type="submit">Ask</button>
  </form>
</div>
<script>
(function(){
  var hist=[], busy=false, started=false;
  var SUG=["How did last month go, and are we hitting our 15% net target?","Which months lost money, and why?","How does this year compare to last year so far?","What's our slowest season?"];
  function msgs(){return document.getElementById('cfo-msgs');}
  function scroll(){var m=msgs();m.scrollTop=m.scrollHeight;}
  function bubble(cls,txt){var d=document.createElement('div');d.className=cls;d.textContent=txt;msgs().appendChild(d);scroll();return d;}
  function renderSug(){SUG.forEach(function(s){var b=document.createElement('button');b.className='cfo-sug';b.type='button';b.textContent=s;b.onclick=function(){send(s);};msgs().appendChild(b);});}
  window.cfoOpen=function(){document.getElementById('cfo-panel').classList.add('open');document.getElementById('cfo-fab').style.display='none';if(!started){started=true;renderSug();}document.getElementById('cfo-in').focus();};
  window.cfoClose=function(){document.getElementById('cfo-panel').classList.remove('open');document.getElementById('cfo-fab').style.display='block';};
  window.cfoAsk=function(){var i=document.getElementById('cfo-in');var v=i.value;i.value='';send(v);return false;};
  function send(q){q=(q||'').trim();if(!q||busy)return;
    var sugs=msgs().querySelectorAll('.cfo-sug');for(var k=0;k<sugs.length;k++){sugs[k].remove();}
    bubble('cfo-u',q);
    busy=true;var sb=document.getElementById('cfo-send');sb.disabled=true;
    var load=bubble('cfo-load','Thinking...');
    fetch('/financials/ask',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q,history:hist})})
      .then(function(r){return r.json().then(function(d){return {ok:r.ok,d:d};});})
      .then(function(res){load.remove();
        if(!res.ok||res.d.error){bubble('cfo-e',res.d.error||'Something went wrong.');}
        else{bubble('cfo-a',res.d.answer);hist.push({role:'user',content:q});hist.push({role:'assistant',content:res.d.answer});if(hist.length>12){hist=hist.slice(-12);}}
      })
      .catch(function(){load.remove();bubble('cfo-e','Could not reach the CFO. Try again.');})
      .then(function(){busy=false;sb.disabled=false;});
  }
})();
</script>
"""

