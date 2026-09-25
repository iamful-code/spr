# HANDOFF: робот сбора спреда (SPR) для Bybit

Резюме для продолжения работы в новой сессии. Предыдущая сессия работала в окружении,
где хосты Bybit были закрыты сетевой политикой, поэтому код написан, но ещё не собран и не
проверен на живых данных. Ветка: `claude/exciting-fermat-lhnv2t`.

## 1. Исходная постановка задачи (дословно от пользователя)

> я хочу написать робота для сбора спреда в стакане. давай возьмем для примера биржу bybit,
> напишем сам механизм, анализировать будем вначале все пары, потом возможно их как-то
> ограничим, я надеюсь тут ты тоже позже предложишь механизм для определения какие пары
> подходят для торговли, а какие нет в данный момент. торговать будем пока бумажно для
> проверки работоспособности. но самое главное и самое важное это механизм анализа всех
> сделок которые мы совершаем и если они убыточны или просто возможно было бы сделать эту
> сделку лучше, то алгоритм должен сам либо предлагать какие изменения надо сделать, либо
> обучаться сам для улучшения своего алгоритма. средой разработки должна быть среда где
> можно создать достойный визуал, а самое главное чтобы это не влияло на скорости расчета
> и отправки/получения данных с биржи. если необходимо перед написанием можешь задать мне
> уточняющие вопросы

### Ответы пользователя на уточняющие вопросы

| Вопрос | Ответ |
|---|---|
| Рынок | **Linear USDT-перпетуалы** (мейкер 0.02%, тейкер 0.055%; круг мейкера ~4 bps; можно шортить) |
| Стек ядра | **Rust ядро + Python аналитика** |
| Визуал | **Веб-дашборд в отдельном процессе** (FastAPI + Plotly, читает только из БД, к ядру не обращается) |
| Самообучение | **Диагностика сделок + автоподбор параметров** (реплей записанного стакана, walk-forward, защита от переобучения) |

## 2. Архитектура (принятые решения и почему)

```
┌──────────────────────────── Rust: core/ (бинарь `spr`) ─────────────────────────────┐
│ bybit/ws.rs  ──► MarketEvent ──► engine.rs (один поток, всё состояние)               │
│ bybit/rest.rs      (mpsc)         ├─ book.rs        локальный стакан (BTreeMap тиков) │
│                                   ├─ stats.rs       медиана спреда, vol, поток сделок  │
│                                   ├─ eligibility.rs фильтр и score символов           │
│                                   ├─ strategy.rs    расчёт котировок (чистая функция) │
│                                   ├─ paper.rs       бумажная биржа (очередь, латентность)│
│                                   ├─ portfolio.rs   позиции, PnL по средней цене       │
│                                   └─ risk.rs        лимиты портфеля, kill-switch       │
│                                   └──► StoreMsg (crossbeam) ──► store.rs (поток SQLite │
│                                                                  + recorder.rs bin-файлы)│
│ replay.rs: те же engine+strategy+paper поверх записанных файлов, выдаёт JSON-метрики  │
│ simfeed.rs: синтетический поток для тестов без сети                                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
              │ data/spr.db (SQLite, WAL)        │ data/md/YYYYMMDD/*.bin (BBO, сделки)
              ▼                                  ▼
┌──────── Python: analytics/spr_analytics ────────┐   ┌──── dashboard (FastAPI, отдельный процесс) ────┐
│ roundtrips.py  FIFO-сведение исполнений          │   │ читает только SQLite, обновляется поллингом     │
│ markouts.py    markout +1s/+5s/+30s/+60s по BBO   │   │ вкладки: обзор, пары, сделки, рекомендации,     │
│ diagnostics.py правила → рекомендации в БД        │   │ оптимизатор                                     │
│ pairs.py       офлайн-скоринг пар → symbols.json  │   └─────────────────────────────────────────────────┘
│ optimizer.py   Optuna × `spr replay`, walk-forward → overrides.json                  │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

Ключевые решения:

- **Одна логика для live и replay.** Стратегия и бумажная биржа работают на одних и тех же
  `MarketEvent`; в replay события читаются из записанных файлов. Поэтому оптимизатор
  проверяет ровно тот код, который торгует.
- **Ядро однопоточное по состоянию**, I/O асинхронный. Парсинг JSON делается в задачах
  WebSocket (многопоточный tokio), движок получает готовые события через `mpsc`.
- **Запись в БД и на диск в отдельном потоке** (`crossbeam-channel` unbounded). Движок никогда
  не ждёт диск.
- **Дашборд не трогает ядро вообще**: отдельный процесс, читает SQLite (WAL позволяет читать
  параллельно с записью).
- **Цены в целых тиках** (`price / tick_size`), чтобы float-шум не создавал дублей уровней.
- **Конфиг перечитывается на лету** по mtime трёх файлов: `config/strategy.toml` (база),
  `config/overrides.json` (пер-символьные переопределения от оптимизатора, ключ `"*"` = для всех),
  `config/symbols.json` (`allow`/`deny`/`scores` от скорера пар). Плохие переопределения
  отклоняются валидацией (`StrategyParams::validate`), а не применяются молча.
- **Bybit orderbook.1** (10 мс) по умолчанию: дёшево для сотен символов, даёт лучшую цену и
  её размер (нужен для оценки очереди). Глубина настраивается (`orderbook_depth`).

### Модель бумажного исполнения (paper.rs)

- Ордер становится активным через `latency_ms` после выставления; отмена тоже действует через
  `latency_ms` (в промежутке ордер может исполниться).
- При активации: post-only проверка (пересекает рынок → `rejected_post_only`); очередь впереди =
  показанный размер на нашей цене (0, если мы внутри спреда).
- Публичная сделка на нашей цене с нужной стороной тейкера сначала съедает очередь впереди,
  остаток исполняет нас. Сделка сквозь нашу цену или движение BBO сквозь неё исполняют остаток.
- Если показанный размер на уровне стал меньше нашей оценки очереди, очередь уменьшается
  (отмены впереди нас).
- Тейкерный выход (для залежавшейся позиции) исполняется по касанию с тейкерской комиссией.

### Логика котирования (strategy.rs)

1. Позиция старше `max_hold_secs` → только выход (`improve` на тик внутрь или `taker`).
2. Волатильность выше `max_vol_bps` → только пассивный выход открытой позиции.
3. Требуемый спред = `max(min_spread_bps, 2*maker_fee_bps + min_edge_bps)`; режим `join`
   (встать в лучшую цену) или `improve` (на тик внутрь, если спред позволяет).
4. Скос по инвентарю: обе котировки сдвигаются против позиции на
   `inventory_skew_bps * (позиция / max_position_notional_usd)`, целыми тиками, без пересечения рынка.
5. Токсичный поток: дисбаланс тейкерских покупок/продаж за окно выше порога → не котируем
   сторону, которую «переедут».
6. Размер `order_notional_usd / mid` с округлением вниз до лота; при достижении лимита позиции
   котируется только сторона выхода; `reduce_only` (символ выпал из активного набора) и запрет
   риск-слоя блокируют только входы, выходы разрешены.

### Фильтр пар (eligibility.rs)

Порядок проверок: deny-список → allow-список → свежесть стакана (5 с) → оборот 24ч ≥
`min_turnover_24h_usd` → прогрев окна → медиана спреда ≥ `max(min_spread_med_bps, min_spread_bps)`
→ сделок/мин ≥ `min_trades_per_min` → `spread_med / vol ≥ min_spread_vol_ratio`.
Score = `spread_med_bps * sqrt(trades_per_min) / (1 + vol_bps)`; при наличии офлайн-score из
`symbols.json` берётся среднее. Движок котирует top-N (`max_active_symbols`) по score,
пересчёт каждые `refresh_secs`.

## 3. Что уже сделано (файлы в репозитории)

| Файл | Состояние |
|---|---|
| `.gitignore` | data/, БД, overrides/symbols.json, target, .venv |
| `config/strategy.toml` | полная базовая конфигурация с комментариями на русском |
| `core/Cargo.toml` | зависимости: tokio, tokio-tungstenite (rustls), reqwest (rustls), serde/serde_json (raw_value), toml, rusqlite (bundled), clap, anyhow, tracing, rand, rand_distr, chrono, crossbeam-channel |
| `core/src/types.rs` | Side, SymbolMeta (округления цены/лота), Bbo, Trade, MarketEvent, Purpose, Fill, OrderDone |
| `core/src/config.rs` | все секции конфига с default'ами, StrategyParams + валидация, Overrides (merge через JSON), SymbolLists, ConfigWatcher (mtime); 2 юнит-теста |
| `core/src/book.rs` | стакан на BTreeMap тиков, snapshot/delta, починка пересечённого стакана; 2 теста |
| `core/src/stats.rs` | посекундные сэмплы спреда/mid, медиана, vol (std лог-доходностей × √60, bps), сделки/мин, дисбаланс потока; 2 теста |
| `core/src/strategy.rs` | `compute_quotes` (см. выше); 6 тестов |
| `core/src/paper.rs` | бумажная биржа (см. выше); 4 теста |
| `core/src/portfolio.rs` | учёт по средней цене, реализованный/нереализованный PnL, переворот позиции, дневной PnL, просадка; 1 тест |
| `core/src/risk.rs` | дневной лимит убытка (latched kill-switch), валовая экспозиция, число позиций |
| `core/src/eligibility.rs` | фильтр и score |

**Важно:** крейт ещё **не собирался** (нет `main.rs`/`lib.rs`, не хватает модулей ниже), так
что тесты не запускались. Первым делом в новой сессии: дописать недостающие модули, `cargo test`,
исправить ошибки компиляции.

Python venv `.venv` (в .gitignore) создавался с пакетами: `numpy pandas fastapi "uvicorn[standard]" optuna scipy pytest`.

## 4. Что не удалось из-за ограничений окружения

- `api.bybit.com`, `stream.bybit.com`, `bybit-exchange.github.io`, `www.bybit.com` были закрыты
  egress-политикой (403 от прокси). Живой прогон невозможен; план был проверять через
  синтетический поток (`simfeed`).
- Документация V5 частично получена через `raw.githubusercontent.com/bybit-exchange/docs`
  (страницы orderbook, trade, instruments-info, tickers). Страница `ws/connect` не нашлась в
  репозитории документации; лимиты подписок взяты из поисковой выдержки официальной документации.
- Запись `core/src/store.rs` была отклонена пользователем в момент прерывания (файла нет).
  Его дизайн описан ниже.

## 5. Факты по Bybit V5 (чтобы не искать заново)

- Public WS linear: `wss://stream.bybit.com/v5/public/linear`. Ping: `{"op":"ping"}` каждые 20 с.
  Подписка: `{"op":"subscribe","req_id":"...","args":["orderbook.1.BTCUSDT","publicTrade.BTCUSDT"]}`.
  Ответ на подписку: `{"success":true,"ret_msg":"","op":"subscribe",...}`.
- Лимиты аргументов: spot — до 10 args на запрос; options — до 2000 на соединение; для
  фьючерсов явного лимита нет, но длина массива `args` на одно соединение ≤ 21 000 символов.
  Поэтому конфиг: `topics_per_connection = 200`, `args_per_subscribe = 50` (несколько соединений).
- `orderbook.{depth}.{symbol}`: linear depth 1 (10 мс), 50 (20 мс), 200/500 (100 мс).
  Сообщение: `{"topic","type":"snapshot"|"delta","ts","data":{"s","b":[["price","size"],...],"a":[...],"u","seq"},"cts"}`.
  Size `"0"` в delta = удалить уровень. `u == 1` или новый `snapshot` = сбросить локальный стакан.
  Для L1 linear: если 3 с нет изменений, snapshot присылается повторно.
- `publicTrade.{symbol}`: `{"topic","type":"snapshot","ts","data":[{"T":ms,"s","S":"Buy"|"Sell" (сторона тейкера),"v":"size","p":"price","L","i","BT"}]}`, массив отсортирован по времени.
- REST `GET /v5/market/instruments-info?category=linear&limit=1000&cursor=...`:
  `result.list[]` с `symbol, contractType ("LinearPerpetual"), status ("Trading"), baseCoin, quoteCoin,
  settleCoin, priceScale, priceFilter.tickSize, lotSizeFilter.{minOrderQty,maxOrderQty,qtyStep,minNotionalValue}`,
  пагинация `result.nextPageCursor`.
- REST `GET /v5/market/tickers?category=linear`: `result.list[]` с `symbol, lastPrice, bid1Price, bid1Size,
  ask1Price, ask1Size, volume24h, turnover24h, fundingRate, openInterest`.
- Комиссии деривативов VIP0: мейкер 0.02%, тейкер 0.055% (в конфиге `[fees]`).

## 6. Что осталось сделать (план)

### Rust (core/)

1. **`store.rs`** — поток записи. `enum StoreMsg { RunStart, Fill{f, realized}, OrderDone, PositionState,
   SymbolStats{...}, Pnl{...}, Event{level,msg}, ParamVersion, MdBbo, MdTrade, Shutdown }`.
   `open_store(db_path, md_dir, run_id, symbols: Vec<String>, record_md) -> StoreHandle{tx}` и
   `null_store()` для replay. Батчи по 500 мс / 2000 сообщений в одной транзакции, `prepare_cached`.
   Схема SQLite (WAL): `runs, fills, orders, symbol_stats, pnl_snapshots, position_state (PK run_id,symbol),
   events, param_versions, recommendations, optimizer_runs, pair_scores` (последние три пишет Python).
   Поля `fills`: run_id, order_id, symbol, side, price, qty, fee, ts, is_maker, purpose, bid, ask, placed_ts,
   mid_at_place, spread_bps_at_place, queue_ahead_initial, inventory_before, param_version, realized_pnl.
2. **`recorder.rs`** — бинарная запись рыночных данных, по одному файлу на UTC-день и запуск:
   `data/md/YYYYMMDD/run{run_id}_bbo.bin` (запись 48 байт LE: ts i64, sym u32, pad u32, bid f64, ask f64,
   bid_qty f64, ask_qty f64), `run{run_id}_trd.bin` (32 байта: ts i64, sym u32, side u8, pad[3], price f64,
   qty f64), `run{run_id}_symbols.json` (id → имя). Коалесценция BBO `bbo_min_interval_ms`.
   Читалка для replay: слить файлы за период, отфильтровать символы, отсортировать по ts.
   В Python читается `numpy.fromfile` с dtype.
3. **`bybit/rest.rs`** — `fetch_instruments` (пагинация, фильтр LinearPerpetual/Trading/USDT) и
   `fetch_tickers` (оборот 24ч) через reqwest.
4. **`bybit/ws.rs`** — менеджер соединений: по `topics_per_connection` топиков на соединение,
   подписка батчами, ping 20 с, реконнект с backoff, парсинг через serde с `RawValue` и
   заимствованными `&str`, отправка `MarketEvent::Book/Trades` в `mpsc`.
5. **`engine.rs`** — единый цикл: `on_market(ev, now_ms)` и `on_tick(now_ms)` (раз в секунду:
   активация/отмены в paper, refresh статистик, пересчёт активного набора, риск, снимки в БД,
   перечитывание конфига каждые 5 с). Пер-символьное состояние: ордер bid/ask, время последнего
   requote, verdict, параметры (merged) и их версия. Логика requote: если желаемая цена/размер
   отличаются и прошло `min_requote_ms` → cancel+place; ордер старше `max_order_age_ms` → снять.
   Live: `now` = системные часы; replay: `now` = ts события.
6. **`replay.rs`** — прогнать движок по записанным данным с параметрами из JSON (CLI-аргумент),
   выдать JSON: net_pnl, gross_pnl, fees, n_fills, n_roundtrips, win_rate, max_drawdown,
   avg_abs_inventory, fill_ratio, pnl по символам.
7. **`simfeed.rs`** — синтетический рынок для N символов: случайное блуждание, меняющийся спред,
   пуассоновские сделки с корреляцией стороны тейкера и будущего движения цены (чтобы
   диагностика adverse selection была проверяема). Работает в реальном времени или ускоренно.
8. **`main.rs`** (clap): `spr run [--config]`, `spr sim [--symbols N] [--speed] [--duration-secs]`,
   `spr replay --from --to [--symbols] [--params-json]`, `spr symbols`.

### Python (analytics/spr_analytics)

- `db.py`, `md.py` (чтение bin через numpy), `roundtrips.py` (FIFO по символу: pnl, комиссия,
  время удержания, захваченный спред в bps), `markouts.py` (mid через +1/+5/+30/+60 с после
  исполнения, знак по стороне; отрицательные = adverse selection).
- `diagnostics.py` — правила → таблица `recommendations` с конкретными изменениями параметров:
  1) markout_5s < −0.5×полуспреда при n≥30 → поднять `min_spread_bps` / включить toxicity-фильтр;
  2) низкая доля исполнений при большой очереди → `quote_mode=improve` или больше `min_requote_ms`;
  3) p90 удержания > `max_hold_secs` и убыточные выходы → меньше `max_position_notional_usd`, больше `inventory_skew_bps`;
  4) символ стабильно убыточен (≥20 кругов) → в deny;
  5) реализованный спред ≪ котируемого → расширить или проверить латентность;
  6) комиссии > 50% валового → больше `order_notional_usd`/`min_edge_bps`;
  7) слишком мало сделок → ниже `min_spread_bps`, больше символов.
- `pairs.py` — офлайн-скоринг по истории + реальным результатам → `config/symbols.json`
  (`allow`, `deny`, `scores`) и таблица `pair_scores`.
- `optimizer.py` — Optuna (TPE) поверх `spr replay`, walk-forward (train 70% / valid 30%),
  цель = net_pnl − λ·max_drawdown − μ·avg_abs_inventory; применять в `overrides.json` только если
  valid-результат лучше текущих параметров на заданный порог; запись в `optimizer_runs`.
- `cli.py`: `python -m spr_analytics analyze|pairs|optimize|clean|report`.
- `dashboard/app.py` + `static/index.html`: FastAPI, endpoints `/api/summary, /api/equity,
  /api/symbols, /api/fills, /api/roundtrips, /api/recommendations, /api/optimizer, /api/events`,
  Plotly с CDN, автообновление 3 с.

### Прочее

- `README.md` на русском: установка, запуск ядра, дашборда, аналитики, оптимизатора, описание
  параметров, дисковый бюджет записи (≈1 ГБ/день при 250 мс коалесценции на всех символах).
- Сквозная проверка: `spr sim` → `analyze` → `optimize` → дашборд; затем живой прогон
  `spr run` при открытом доступе к Bybit.

## 7. Как собрать и проверить

```bash
# Rust (после добавления main.rs и недостающих модулей)
cd core && cargo test && cargo build --release
./target/release/spr sim --symbols 20 --duration-secs 600      # без сети
./target/release/spr run --config ../config/strategy.toml       # живой поток Bybit (paper)

# Python
python3 -m venv .venv && . .venv/bin/activate
pip install numpy pandas fastapi "uvicorn[standard]" optuna scipy pytest
cd analytics && pip install -e . && python -m spr_analytics analyze
uvicorn spr_analytics.dashboard.app:app --port 8000            # http://localhost:8000
```

Сетевые хосты, которые нужны окружению: `api.bybit.com`, `stream.bybit.com`
(и `bybit-exchange.github.io` для документации).
