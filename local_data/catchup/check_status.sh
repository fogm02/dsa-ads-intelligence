#!/bin/bash
# Zkontroluje stav Meta catch-upu.
# Použití:  bash catchup/check_status.sh
#       nebo: ./catchup/check_status.sh

cd "$(dirname "$0")/.."

DONE=$(ls catchup/silver/*.csv 2>/dev/null | wc -l | xargs)
RUNNING=$(ps aux | grep catchup_meta | grep -v grep | wc -l | xargs)
TARGET=15

echo "============================================"
if [[ "$RUNNING" == "0" ]]; then
  if [[ "$DONE" -ge "$TARGET" ]]; then
    echo "  STAV: ✅ DOKONČENO ($DONE/$TARGET sektorů)"
  else
    echo "  STAV: ⚠️  ZASTAVENO ($DONE/$TARGET sektorů — proces neběží)"
  fi
else
  echo "  STAV: ⏳ BĚŽÍ ($DONE/$TARGET sektorů hotovo)"
fi
echo "============================================"
echo ""
echo "Hotové sektory:"
for f in catchup/silver/*.csv; do
  if [ -f "$f" ]; then
    sector=$(basename "$f" .csv)
    # CSV parser — Meta creative_body obsahuje \n, takže wc -l je zavádějící
    rows=$(python3 -c "import csv; print(sum(1 for _ in csv.DictReader(open('$f', encoding='utf-8-sig'))))" 2>/dev/null)
    printf "  %-22s %5d reklam\n" "$sector" "$rows"
  fi
done | sort

if [[ "$RUNNING" == "1" ]]; then
  echo ""
  echo "Aktuálně zpracovává:"
  tail -5 /tmp/catchup_progress.log 2>/dev/null | grep -E "^===|complete|INCOMPLETE|ERROR|Total"
fi
