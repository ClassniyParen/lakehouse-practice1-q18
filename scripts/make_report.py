"""Fill the teacher's seven sections using observed results, with explicit provenance."""
import argparse
import csv
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def make(engine):
    is_trino=engine=='trino'
    directory=ROOT/'results'/engine
    if is_trino:
        assert (directory/'SUCCESS').is_file(), 'Actual pipeline success marker required'
        assert (ROOT/'results/bi-bronze.json').is_file(), 'BI Bronze query evidence required'
        env=load(directory/'environment.json')
        metrics=load(directory/'benchmark.json');layers=load(directory/'layers.json')
        checks=load(directory/'checks.json');evolution=load(directory/'evolution.json')
        provenance=f"Trino {env['version']}, Iceberg и MinIO. Среда выполнения: {env.get('run_context','Docker host')}."
        status='Контейнерный стек, ELT, бенчмарк, изменение схемы, time travel, rollback и обращение BI к Bronze выполнены. Фактические журналы находятся в `results/trino/`, результат BI — в `results/bi-bronze.json`.'
    else:
        env=load(directory/'measurements.json');metrics=env['metrics'];layers=env['layers'];checks=env['checks']
        provenance=f"DuckDB {env['version']}, {env['platform']}, 4 потока, лимит памяти 2 GB, локальные файлы."
        status='Проверены SQL Q18, слои и локальный бенчмарк. Docker/WSL на компьютере отсутствуют; контейнерный запуск ещё не подтверждён. Таблицы ниже содержат фактические локальные замеры DuckDB, а не Trino/MinIO. Разделы про Iceberg и BI описывают подготовленные сценарии; их выполнение требует запуска `start.ps1` или GitHub Actions.'
    with (directory/'q18_top100.csv').open(encoding='utf-8-sig',newline='') as f:
        result=list(csv.DictReader(f))
    q=(ROOT/'sql/q18.sql').read_text(encoding='utf-8').replace('{schema}','lake.bronze')
    lines=[
        '# Отчёт по практической работе №1',
        '', '**Тарасов Глеб Святославович**', '',
        'Развёртывание локального Lakehouse, ELT-преобразования и бенчмарк форматов и сжатия. Вариант 3 — Q18 Large Volume Customer.',
        '', '## Статус и происхождение результатов', '',status,'',provenance,
        f"Дата замеров UTC: {env['timestamp_utc']}. Все размеры в таблицах — десятичные МБ: 1 МБ = 1 000 000 байт.",
        '', '## 1. Вариант и масштаб', '',
        '| Поле | Значение |','|---|---|',
        '| ФИО как в VK Education | Тарасов Глеб Святославович |',
        '| Вариант | 3 — Q18 Large Volume Customer |',f"| Масштаб | sf{env['sf']:g} |",
        '| Обоснование масштаба | SF1 даёт около 6 млн позиций заказов: достаточно для сравнения кодеков и агрегаций, при этом обработка помещается на одном компьютере. Для всех слоёв и конфигураций использован один исходный масштаб. Это учебный микробенчмарк, а не официальная публикация TPC-H. |',
        '', '## 2. Сервисы и обоснование выбора', '',
        ('S3-сервис minio развёрнут на Silo, поддерживаемом форке MinIO: старый публичный образ MinIO недоступен. Запущенные сервисы подтверждены `results/trino/docker-ps.txt`; версии образов сохранены в `docker-images.txt`.' if is_trino else 'S3-сервис minio использует образ pgsty/silo RELEASE.2026-09-03T13-18-01Z — поддерживаемый форк MinIO из актуального примера Lakekeeper; старый образ MinIO недоступен. Ниже приведён состав подготовленного Docker-стека. Фактический запуск контейнеров пока не подтверждён.'), '',
        '| Сервис | Роль в стеке | Почему выбран |','|---|---|---|',
        '| Silo совместимый форк MinIO | S3-совместимое хранилище файлов Iceberg | Даёт S3 API на одной машине без облачного аккаунта; один и тот же набор файлов доступен разным движкам. Локальная файловая система проще, но не проверяет S3-интеграцию. |',
        '| PostgreSQL 16 | Служебные метаданные Lakekeeper | Транзакции обеспечивают согласованность состояния каталога; это не хранилище строк TPC-H. В отличие от встроенной временной БД состояние сохраняется в Docker volume. |',
        '| Lakekeeper 0.10.0 | Iceberg REST Catalog | Открытый REST-интерфейс отделяет каталог от Trino и позволяет подключать другие движки к тем же таблицам. Hive Metastore потребовал бы другой протокол и конфигурацию. |',
        '| Trino 484 | ELT, SQL и генератор TPC-H | В одном движке доступны tpch и iceberg; данные копируются через SQL CTAS без отдельного Spark-кластера. На SF1 отдельный распределённый ETL-движок избыточен. |',
        '| Apache Superset 4.1.2 | BI поверх Trino | Открытый BI с SQL Lab и драйвером Trino; удобно проверить Bronze SQL-запросом и исследовать Gold без копирования данных в BI. |',
        '| Python runner | Автоматизация и сбор доказательств | Стандартная библиотека выполняет запросы через HTTP Trino, сохраняет query ID, результаты, времена и проверки. |',
        '', '### Команды развёртывания', '',
        'Полная конфигурация находится в `compose.yaml`, каталоги Trino — в `config/trino/catalog/`. Инициализация бакета, миграции PostgreSQL, bootstrap и создание warehouse выполняются автоматически.',
        '', '```powershell', '.\\start.ps1 -ScaleFactor 1 -Repeats 5', '```', '',
        'Эквивалентные основные шаги:', '', '```bash',
        'docker compose up -d trino',
        'docker compose --profile tools run --rm runner python scripts/run_trino.py --sf 1 --repeats 5',
        'docker compose --profile bi up -d --build superset',
        'docker compose ps --all',
        'docker compose --profile tools run --rm runner python scripts/make_report.py --engine trino', '```', '',
        'Сервисы используют одну внутреннюю сеть Compose. Данные MinIO, PostgreSQL и Superset сохраняются в именованных volumes. Учебные пароли находятся в конфигурации; опубликованные порты привязаны к 127.0.0.1.',
        '', '## 3. Разбор запроса', '',
        'Q18 находит **заказы**, в которых суммарное количество товара строго больше 300, соединяет их с клиентами и выбирает до 100 самых дорогих заказов. Один клиент может появиться несколько раз. `totalprice` — стоимость всего заказа; её нельзя суммировать после соединения с позициями, иначе сумма задвоится.', '',
        '| Таблица | Нужные колонки | Использование |','|---|---|---|',
        '| customer | custkey, name | Ключ соединения с заказом и имя для вывода |',
        '| orders | orderkey, custkey, orderdate, totalprice | Соединение с клиентом и позициями, сортировка по стоимости и дате |',
        '| lineitem | orderkey, quantity; linenumber для качества данных | Сумма количества по заказу и условие > 300; уникальность позиции по (orderkey, linenumber) |',
        '', '### Исходная форма Q18', '', '```sql',q.strip()+';', '```', '',
        'Дополнительная сортировка по orderkey делает порядок при совпадающих цене и дате воспроизводимым. Основные условия и первые два ключа сортировки соответствуют варианту.',
        '', '## 4. Медальон', '',
        'Bronze сохраняет все колонки трёх исходных таблиц. Silver оставляет нужные поля, применяет trim к имени, явно задаёт дату и decimal, проверяет обязательные ключи, неотрицательную стоимость и положительное количество. Крупные заказы сначала выделяются агрегацией, затем обогащаются именем клиента и атрибутами заказа. Gold материализует до 100 строк по заданной сортировке.', '',
        'У сгенерированных TPC-H данных ошибки и дубликаты не добавлялись искусственно. Скрипт проверяет дубликаты и ссылочную целостность и останавливается при нарушении. Число отклонённых фильтрами Silver строк записано в проверках. При сравнении Trino DOUBLE с DECIMAL денежные значения и количество нормализуются к заявленному масштабу 0,01; HTTP кодирует DECIMAL строкой, а DOUBLE числом.', '',
        ('Размер — сумма `file_size_in_bytes` из Iceberg `$files` текущего снимка. Старые снимки и служебные метаданные в эту сумму не входят. `SHOW STATS.data_size` не используется как размер объектов MinIO.' if is_trino else 'Размеры ниже получены по фактической длине локальных файлов Parquet; это не размеры объектов MinIO.'), '',
    ]
    reasons={
        'bronze':'Parquet сохраняет типы и читает только нужные колонки. ZSTD уменьшает сырой набор без дорогостоящей распаковки GZIP. Все колонки сохранены для повторного ELT.',
        'silver':'Parquet и ZSTD подходят для повторных соединений и агрегаций. Проекция уменьшает данные; денежные значения представлены decimal без двоичных погрешностей.',
        'gold':'Parquet и ZSTD сохраняют единый способ чтения BI. Витрина мала, поэтому экономия на сжатии здесь вторична; важнее готовая агрегация.'}
    for layer,title in [('bronze','Bronze сырой слой'),('silver','Silver очищенный слой'),('gold','Gold витрина')]:
        lines+=['### '+title,'','| Таблица | Колонки | Строк | Вес МБ | Формат | Сжатие | Партиционирование | Обоснование |','|---|---|---:|---:|---|---|---|---|']
        for row in layers:
            if row['layer']==layer:
                lines.append(f"| {row['table']} | {', '.join(row['columns'])} | {row['rows']:,} | {row['bytes']/1e6:.6f} | PARQUET | ZSTD | нет | {reasons[layer]} |")
        lines+=['']
    lines+=[
        'Партиционирование не использовано: Q18 не ограничивает дату и всё равно читает все заказы. На SF1 разбиение по дате добавило бы мелкие файлы без pruning для этого запроса; по orderkey — слишком много разделов. При росте объёма и появлении временных фильтров решение следует пересмотреть.', '',
        f"Витрина содержит **{len(result)} строк**. LIMIT 100 — верхняя граница, а не требование искусственно получить ровно 100 строк. Gold совпал с исходным Q18 для того же генератора и масштаба.", '',
        '### Проверки качества', '', '| Проверка | Результат |','|---|---|']
    for key,value in checks.items():
        lines.append(f'| {key} | {value} |')
    lines+=['','## 5. Тесты производительности','',
        f"Источник замеров: {provenance} Для каждой конфигурации выполнено {env['repeats']} повторов записи и {env['repeats']} измеряемых запросов после одного прогрева. Порядок конфигураций перемешан с seed 18. В таблице показаны медианы и диапазон времени чтения.", '',
        ('Время записи — сумма клиентского времени трёх CTAS, включая генерацию TPC-H и commit Iceberg, без DROP TABLE. Время запроса — от отправки SQL до получения всех строк по HTTP. Точные серверные stats и query ID сохранены отдельно. Размер — активные файлы трёх Bronze-таблиц каждой конфигурации; количество файлов записано для контроля сопоставимости.' if is_trino else 'Время записи — последовательный экспорт трёх полных исходных таблиц из памяти; время запроса — чтение файлов и полное получение результата. Для CSV задана явная схема. Размер — сумма трёх файлов. Все Parquet-файлы используют row group 122880. Уровни сжатия — значения по умолчанию DuckDB 1.4.3.'), '',
        'Кеш операционной системы не сбрасывался: измеряется повторное чтение. Результаты не являются cold-cache тестом и не переносятся напрямую на другой компьютер, SQL-движок или распределённый кластер. Между повторениями проверяется полное совпадение результата.', '',
        '| Формат | Сжатие | Запись с | Запрос с | Диапазон запроса с | Размер Bronze МБ |',
        '|---|---|---:|---:|---:|---:|']
    for m in sorted(metrics,key=lambda m:(m['format'],m['codec'])):
        lines.append(f"| {m['format']} | {m['codec']} | {m['write_median_s']:.4f} | {m['query_median_s']:.4f} | {m['query_min_s']:.4f}–{m['query_max_s']:.4f} | {m['bytes']/1e6:.3f} |")
    z=next(m for m in metrics if m['format']=='PARQUET' and m['codec']=='ZSTD')
    g=next(m for m in metrics if m['format']=='PARQUET' and m['codec']=='GZIP')
    lines+=['',f"В этом запуске Parquet ZSTD: {z['bytes']/1e6:.3f} МБ и {z['query_median_s']:.4f} с на Q18; GZIP: {g['bytes']/1e6:.3f} МБ и {g['query_median_s']:.4f} с. Отношение времени записи GZIP/ZSTD — {g['write_median_s']/z['write_median_s']:.2f}; отношение времени запроса — {g['query_median_s']/z['query_median_s']:.2f}. Выбор ZSTD для основного медальона основан на балансе размера, записи и чтения; минимальный размер сам по себе не означает лучшую производительность.", '',
        '## 6. Осмысление и работа с инструментами','','### 6.1. Объяснение решений','',
        '| Вопрос | Ответ |','|---|---|',
        '| Зачем lakehouse | Объектное хранилище отделяет данные от вычислительных движков; открытый формат файлов позволяет нескольким инструментам читать общий набор. Iceberg добавляет согласованные снимки, эволюцию схемы и транзакционные изменения. На SF1 обычная аналитическая БД была бы проще; lakehouse здесь нужен для освоения архитектуры и возможности дальнейшего роста. |',
        '| Schema evolution на Silver | Демонстрация выполняется на отдельной копии silver.customer_orders в evolution_demo.orders_demo: ADD COLUMN audit_note varchar, затем RENAME COLUMN в processing_note. У существующих строк значение NULL, пути data files должны остаться прежними: меняются метаданные, а не старые файлы. |',
        '| Time travel и восстановление | Сохраняется snapshot_id, из копии удаляется один заказ, старый снимок читается через FOR VERSION AS OF, затем rollback_to_snapshot восстанавливает строки. Скрипт проверяет полное совпадение восстановленных шести исходных колонок. Схема и снимки данных — разные механизмы; откат данных не следует использовать как способ отката DDL. |', '',
        'Команды, которые исполняет сценарий (snapshot_id и orderkey берутся из реальных запросов, а не заданы случайными числами):', '',
        '```sql',
        'ALTER TABLE lake.evolution_demo.orders_demo ADD COLUMN audit_note varchar;',
        'ALTER TABLE lake.evolution_demo.orders_demo RENAME COLUMN audit_note TO processing_note;',
        'SELECT snapshot_id FROM lake.evolution_demo."orders_demo$snapshots"',
        'ORDER BY committed_at DESC LIMIT 1;',
        '-- Далее Python подставляет полученный snapshot_id и выбранный orderkey:',
        '-- DELETE FROM lake.evolution_demo.orders_demo WHERE orderkey = <orderkey>;',
        '-- SELECT * FROM lake.evolution_demo.orders_demo FOR VERSION AS OF <snapshot_id>;',
        '-- ALTER TABLE lake.evolution_demo.orders_demo EXECUTE rollback_to_snapshot(<snapshot_id>);',
        '```','']
    if is_trino:
        lines+=['Фактический результат:', '', '```json',json.dumps(evolution,ensure_ascii=False,indent=2),'```','']
    else:
        lines+=['Статус демонстрации Iceberg: сценарий подготовлен, но до фактического запуска Docker не считается выполненным.','']
    lines+=['### 6.2. Подключение BI к Bronze','',
        '| Инструмент | Причина выбора |','|---|---|',
        '| Apache Superset | SQL Lab поддерживает Trino через SQLAlchemy; Bronze и Gold доступны без переноса в отдельную БД. На одной машине это проще, чем запуск второго большого SQL-движка. |','',
        'Развёртывание: `docker compose --profile bi up -d --build superset`. Скрипт `bi/setup.py` регистрирует подключение `trino://student@trino:8080/lake/bronze`, четыре датасета и выполняет запрос Bronze из BI-контейнера. Фактический результат сохраняется в `results/bi-bronze.json`.', '',
        'При локальном запуске интерфейс доступен по http://localhost:8088, пользователь `student`, учебный пароль `Practice1-local!`. В SQL Lab выбираются Database → Lakehouse и Schema → bronze. Для просмотра витрины можно выполнить SELECT из lake.gold.q18_top100. В GitHub Actions интерфейс наружу не публикуется; подтверждением служит консольный запрос из BI-контейнера.', '',
        '```sql',
        'SELECT orderkey, sum(quantity) AS qty',
        'FROM lake.bronze.lineitem',
        'GROUP BY orderkey HAVING sum(quantity) > 300',
        'ORDER BY qty DESC, orderkey LIMIT 10;', '```','',
        ('Результат консольного обращения Superset к Bronze:', '```json',json.dumps(load(ROOT/'results/bi-bronze.json'),ensure_ascii=False,indent=2),'```','') if is_trino else 'Статус BI: сценарий и подключение подготовлены; фактическое выполнение пока не подтверждено.' ]
    # Flatten the optional parallel content above.
    flat=[]
    for item in lines:
        flat.extend(item if isinstance(item,tuple) else [item])
    flat+=['','## 7. Выводы','',
        'Медальон отделяет неизменённый исходный набор от очищенных данных и готового бизнес-результата. Для Q18 предварительная агрегация lineitem позволяет сначала выбрать крупные заказы и затем обогатить их данными клиентов. Колонный формат сокращает чтение ненужных полей, а выбор кодека следует делать по совокупности размера, времени записи и времени запроса. На SF1 партиционирование по дате не помогает Q18, потому что в запросе нет ограничения периода. Проверки равенства с исходным SQL и сохранённые замеры делают результат воспроизводимым.', '',
        '## Источники и воспроизведение','',
        '- Форма отчёта преподавателя: https://docs.google.com/document/d/1lI_2HJJJT1YDT8vIB3ZICtyeQH9clx32/edit',
        '- Lakekeeper, официальный пример: https://github.com/lakekeeper/lakekeeper/tree/v0.10.0/examples/minimal',
        '- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html',
        '- Trino S3 configuration: https://trino.io/docs/current/object-storage/file-system-s3.html',
        '- DuckDB TPC-H: https://duckdb.org/docs/stable/core_extensions/tpch',
        '', 'Код, исходные SQL и данные замеров находятся в этом репозитории. Большие генерируемые файлы исключены из Git: они воспроизводятся генератором. Репозиторий: https://github.com/ClassniyParen/lakehouse-practice1-q18','']
    (ROOT/'отчёт.md').write_text('\n'.join(flat),encoding='utf-8')
    print('Written отчет.md with provenance:',engine)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--engine',choices=['local','trino'],default='local')
    make(p.parse_args().engine)
