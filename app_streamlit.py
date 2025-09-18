#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app_streamlit

Веб‑интерфейс на Streamlit для CSV‑валидации. Предоставляет загрузку/предпросмотр
файла, подсчёт строк для прогресса и запуск внешнего CLI-скрипта (streaming_csv_parser.py)
через subprocess с разбором stdout для обновления прогресса.

Артефакты раскладываются по каталогам из config.py (DATA_DIR/EXPORT_DIR/BAD_DIR/LOGS_DIR).
См. README.md для подробностей.
"""

import io
import os
import csv
import time
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Tuple, Optional, List

import chardet
import streamlit as st
from datetime import datetime
import uuid
from config import EXPORT_DIR, BAD_DIR, DATA_DIR, LOGS_DIR

SAMPLE_BYTES = 100_000

def get_session_log_path() -> Path:
    if 'log_path' not in st.session_state:
        LOGS_DIR.mkdir(exist_ok=True)
        sid = st.session_state.get('_session_id') or str(uuid.uuid4())[:8]
        st.session_state['_session_id'] = sid
        fname = f"session_{sid}_{int(time.time())}.log"
        st.session_state['log_path'] = LOGS_DIR / fname
    return st.session_state['log_path']

def write_log(msg: str) -> None:
    path = get_session_log_path()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with path.open('a', encoding='utf-8') as f:
            f.write(f"{ts} {msg}\n")
    except Exception:
        pass


# --------- Core helpers ---------

def sanitize_filename(name: str) -> str:
    name = os.path.basename(name or "uploaded.csv")
    # Replace risky characters
    safe = ''.join(c if c.isalnum() or c in ('-', '_', '.', ' ') else '_' for c in name)
    if not safe.strip():
        safe = 'uploaded.csv'
    return safe


def save_uploaded_to_disk(uploaded_file, target_dir: Path) -> Path:
    """Save the uploaded file to target_dir streaming in chunks. Returns saved path."""
    target_dir.mkdir(parents=True, exist_ok=True)
    original_name = sanitize_filename(getattr(uploaded_file, 'name', 'uploaded.csv'))
    stem = Path(original_name).stem
    suffix = Path(original_name).suffix or '.csv'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = target_dir / f"{stem}_{ts}{suffix}"
    try:
        uploaded_file.seek(0)
    except Exception:
        pass
    with open(out_path, 'wb') as out:
        # stream copy in chunks of 8MB
        try:
            shutil.copyfileobj(uploaded_file, out, length=8 * 1024 * 1024)
        except Exception:
            # some UploadedFile objects require reading in manual chunks
            uploaded_file.seek(0)
            while True:
                chunk = uploaded_file.read(8 * 1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    try:
        uploaded_file.seek(0)
    except Exception:
        pass
    return out_path


def read_first_n_lines_bytes(file_like, n: int = 10000, chunk_size: int = 1024 * 1024) -> bytes:
    """Read from the beginning up to n lines from a binary file-like object and return bytes."""
    try:
        file_like.seek(0)
    except Exception:
        pass
    chunks: list[bytes] = []
    lines_seen = 0
    while True:
        chunk = file_like.read(chunk_size)
        if not chunk:
            break
        chunks.append(chunk)
        lines_seen += chunk.count(b'\n')
        if lines_seen >= n:
            break
    data = b''.join(chunks)
    # Trim to the position of the nth newline to avoid reading beyond n lines
    if lines_seen >= n:
        # find index of nth newline
        count = 0
        idx = -1
        for i, b in enumerate(data):
            if b == 0x0A:  # '\n'
                count += 1
                if count == n:
                    idx = i + 1
                    break
        if idx != -1:
            data = data[:idx]
    try:
        file_like.seek(0)
    except Exception:
        pass
    return data

def detect_encoding_and_reset(file_obj) -> str:
    pos = file_obj.tell()
    head = file_obj.read(SAMPLE_BYTES)
    if isinstance(head, str):
        # already text
        file_obj.seek(pos)
        return 'utf-8'
    enc = chardet.detect(head)["encoding"] or "utf-8"
    if enc and enc.lower() == 'ascii':
        enc = 'utf-8'
    file_obj.seek(0)
    return enc


def detect_input_delimiter(text_stream, max_lines: int = 30) -> str:
    pos = text_stream.tell()
    lines = [text_stream.readline() for _ in range(max_lines)]
    sample = ''.join(lines)
    text_stream.seek(pos)

    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample)
        return dialect.delimiter
    except csv.Error:
        candidates = [',', ';', '\t', '|']
        counts = {d: sum(line.count(d) for line in lines) for d in candidates}
        return max(counts, key=counts.get)


def analyze_csv_stats_stream(text_stream, max_lines: int = 10000, provided_delimiter: Optional[str] = None):
    """
    Анализирует первые max_lines строк потока для определения оптимального разделителя
    и модального количества колонок. Возвращает (delimiter, header_cols, modal_cols, total_rows).
    Бросает ValueError при ошибках.
    Улучшения:
    - Добавлен кандидат из csv.Sniffer().
    - Если все кандидаты дают 1 колонку, используем частотный фоллбек по среднему числу вхождений разделителя.
    """
    import io as _io
    pos = text_stream.tell()
    try:
        lines = []
        for _ in range(max_lines):
            line = text_stream.readline()
            if not line:
                break
            lines.append(line)
        if not lines:
            raise ValueError("Файл пустой — нет строк для анализа")

        base_candidates = [',', ';', '\t', '|']
        candidates: List[str] = []
        if provided_delimiter:
            candidates.append(provided_delimiter)
        # Попробуем sniffer на сэмпле
        try:
            sniffed = csv.Sniffer().sniff(''.join(lines)).delimiter
            if sniffed and sniffed not in candidates:
                candidates.append(sniffed)
        except Exception:
            pass
        for d in base_candidates:
            if d not in candidates:
                candidates.append(d)

        best = None  # (score, delimiter, header_cols, modal_cols, total_rows, modal_count)
        freq_counts = {}
        sample_text = ''.join(lines)
        for d in candidates:
            try:
                freq_counts[d] = sum(line.count(d) for line in lines) / max(1, len(lines))
                reader = csv.reader(_io.StringIO(sample_text), delimiter=d, quoting=csv.QUOTE_MINIMAL)
                lengths = [len(row) for row in reader]
                if not lengths:
                    continue
                header_cols = lengths[0]
                data_lengths = lengths[1:] if len(lengths) > 1 else []
                if data_lengths:
                    from collections import Counter
                    cnt = Counter(data_lengths)
                    modal_cols, modal_count = max(cnt.items(), key=lambda kv: (kv[1], kv[0]))
                else:
                    modal_cols, modal_count = header_cols, 0
                total_rows = len(lengths)
                score = (modal_count / max(1, len(data_lengths))) if data_lengths else 1.0
                rank = (score, modal_count, modal_cols)
                if (best is None) or (rank > (best[0], best[5], best[3])):
                    best = (score, d, header_cols, modal_cols, total_rows, modal_count)
            except Exception:
                continue
        if best is None:
            d = provided_delimiter or ','
            reader = csv.reader(_io.StringIO(sample_text), delimiter=d, quoting=csv.QUOTE_MINIMAL)
            lengths = [len(row) for row in reader]
            if not lengths:
                raise ValueError("Не удалось разобрать первые строки файла")
            header_cols = lengths[0]
            data_lengths = lengths[1:] if len(lengths) > 1 else []
            if data_lengths:
                from collections import Counter
                cnt = Counter(data_lengths)
                modal_cols = max(cnt, key=cnt.get)
            else:
                modal_cols = header_cols
            return d, header_cols, modal_cols, len(lengths)

        score, delimiter, header_cols, modal_cols, total_rows, modal_count = best
        if modal_cols == 1 and freq_counts:
            # Фоллбек: выберем разделитель с наибольшей средней частотой
            best_d = max(freq_counts.items(), key=lambda kv: kv[1])[0]
            reader = csv.reader(_io.StringIO(sample_text), delimiter=best_d, quoting=csv.QUOTE_MINIMAL)
            lengths = [len(row) for row in reader]
            if lengths:
                header_cols = lengths[0]
                data_lengths = lengths[1:] if len(lengths) > 1 else []
                if data_lengths:
                    from collections import Counter
                    cnt = Counter(data_lengths)
                    modal_cols = max(cnt, key=cnt.get)
                else:
                    modal_cols = header_cols
                total_rows = len(lengths)
                delimiter = best_d
        return delimiter, header_cols, modal_cols, total_rows
    finally:
        text_stream.seek(pos)


def process_csv_stream(
    bin_file,
    export_delimiter: str,
    input_encoding: Optional[str] = None,
    input_delimiter: Optional[str] = None,
    progress_callback=None,
) -> Tuple[io.BytesIO, io.BytesIO, io.BytesIO, int, int, int]:
    # Detect encoding
    encoding = input_encoding or detect_encoding_and_reset(bin_file)
    text_stream = io.TextIOWrapper(bin_file, encoding=encoding, errors='ignore', newline='')

    # Статистический анализ первых 10000 строк: определение разделителя и колонок
    det_delim, header_cols_stat, modal_cols_stat, stat_rows = analyze_csv_stats_stream(
        text_stream, max_lines=10000, provided_delimiter=input_delimiter
    )
    if header_cols_stat != modal_cols_stat:
        raise ValueError(
            f"Несоответствие структуры: в заголовке {header_cols_stat} колонок, "
            f"а статистически определено {modal_cols_stat} на первых {stat_rows} строках"
        )
    # Перематываем поток в начало перед основной обработкой
    text_stream.seek(0)

    # Prepare writers
    clean_buf = io.StringIO()
    bad_buf = io.StringIO()
    bad_raw_buf = io.StringIO()

    clean_writer = csv.writer(clean_buf, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)
    bad_writer = csv.writer(bad_buf, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)

    # Bad header
    bad_writer.writerow(["Номер_строки", "Тип_ошибки", "Описание_ошибки", "Содержимое_строки"])

    reader = csv.reader(text_stream, delimiter=det_delim, quoting=csv.QUOTE_MINIMAL)

    valid_count = 0
    bad_count = 0
    total_count = 0

    # Читаем и записываем заголовок
    try:
        header = next(reader)
        expected_columns = len(header)
        clean_writer.writerow(header)
        total_count += 1
    except StopIteration:
        # пустой файл
        clean_bytes = io.BytesIO(clean_buf.getvalue().encode('utf-8'))
        bad_bytes = io.BytesIO(bad_buf.getvalue().encode('utf-8'))
        bad_raw_bytes = io.BytesIO(bad_raw_buf.getvalue().encode('utf-8'))
        return clean_bytes, bad_bytes, bad_raw_bytes, valid_count, bad_count, total_count

    # После заголовка переходим на построчную обработку с возможностью склейки 2 строк
    import io as _io

    def _parse_row(text: str) -> Optional[List[str]]:
        try:
            r = next(csv.reader(_io.StringIO(text), delimiter=det_delim, quoting=csv.QUOTE_MINIMAL))
            return r
        except StopIteration:
            return []
        except Exception:
            return None

    # Номер физической строки (после заголовка начинаем с 2)
    phys_line_no = 2
    buffered_line = None  # сюда кладём следующую строку, если склейка не удалась
    while True:
        # Берём строку из буфера, если он есть, иначе читаем из файла
        if buffered_line is not None:
            cur_line = buffered_line
            buffered_line = None
            from_buffer = True
        else:
            cur_line = text_stream.readline()
            from_buffer = False
        if not cur_line:
            break
        if not from_buffer:
            total_count += 1  # считаем только реально прочитанные строки

        row = _parse_row(cur_line)
        if row is not None and len(row) == expected_columns:
            # Валидная строка — сразу записываем
            clean_writer.writerow(row)
            valid_count += 1
            phys_line_no += 1
        else:
            # Невалидная — пробуем склеить ровно с одной следующей строкой
            next_line = text_stream.readline()
            if next_line:
                total_count += 1
                combined = cur_line.rstrip('\r\n') + next_line.lstrip('\r\n')
                combined_row = _parse_row(combined)
                if combined_row is not None and len(combined_row) == expected_columns:
                    # Склейка удалась — считаем валидной записью и потребляем обе строки
                    clean_writer.writerow(combined_row)
                    valid_count += 1
                    phys_line_no += 2
                else:
                    # Склейка не помогла — текущую строку считаем битой,
                    # а следующую вернём в буфер для повторной нормальной обработки
                    bad_len = (len(row) if row is not None else '—')
                    content = cur_line.rstrip('\r\n')
                    if content.strip() == "":
                        desc = "Пустая строка"
                    elif bad_len == '—':
                        desc = f"Неверная строка (не удалось распарсить, ожидалось {expected_columns} столбцов)"
                    elif bad_len < expected_columns:
                        desc = f"Неверное количество столбцов: {bad_len} вместо {expected_columns}"
                    else:
                        desc = f"Неверное количество столбцов: {bad_len} вместо {expected_columns}"
                    bad_writer.writerow([phys_line_no, "Ошибка_структуры", desc, content])
                    # В файл сырых ошибок пишем строку без изменений
                    bad_raw_buf.write(cur_line)
                    bad_count += 1
                    phys_line_no += 1
                    buffered_line = next_line  # вернём следующую строку на повторную обработку
            else:
                # Нет следующей строки — фиксируем как битую текущую
                content = cur_line.rstrip('\r\n')
                if content.strip() == "":
                    desc = "Пустая строка"
                else:
                    bad_len = (len(row) if row is not None else '—')
                    desc = (
                        f"Неверное количество столбцов: {bad_len} вместо {expected_columns}"
                        if isinstance(bad_len, int) else f"Неверная строка (ожидалось {expected_columns} столбцов)"
                    )
                bad_writer.writerow([phys_line_no, "Ошибка_структуры", desc, content])
                # В файл сырых ошибок пишем строку без изменений
                bad_raw_buf.write(cur_line)
                bad_count += 1
                phys_line_no += 1

        if progress_callback and (valid_count + bad_count) % 5000 == 0:
            progress_callback(valid_count, bad_count, total_count)

    clean_bytes = io.BytesIO(clean_buf.getvalue().encode('utf-8'))
    bad_bytes = io.BytesIO(bad_buf.getvalue().encode('utf-8'))
    bad_raw_bytes = io.BytesIO(bad_raw_buf.getvalue().encode('utf-8'))
    return clean_bytes, bad_bytes, bad_raw_bytes, valid_count, bad_count, total_count


def preview_first_rows(bin_bytes: bytes, encoding: Optional[str] = None, delimiter: Optional[str] = None,
                       limit: int = 200) -> Tuple[List[str], List[List[str]], str, str]:
    """Return header, rows, detected_encoding, detected_delimiter for preview."""
    bio = io.BytesIO(bin_bytes)
    enc = encoding or detect_encoding_and_reset(bio)
    text_stream = io.TextIOWrapper(bio, encoding=enc, errors='ignore', newline='')
    delim = delimiter or detect_input_delimiter(text_stream)

    reader = csv.reader(text_stream, delimiter=delim, quoting=csv.QUOTE_MINIMAL)
    try:
        header = next(reader)
    except StopIteration:
        return [], [], enc, delim

    rows: List[List[str]] = []
    for i, row in enumerate(reader, start=1):
        rows.append(row)
        if i >= limit:
            break
    return header, rows, enc, delim


def read_log_tail(path: Path, max_lines: int = 500) -> str:
    if not path.exists():
        return "Лог-файл пока не создан. Запустите обработку или скрипт для появления логов."
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        return ''.join(lines[-max_lines:])
    except Exception as e:
        return f"Не удалось прочитать логи: {e}"


def count_total_lines(path: Path, chunk_size: int = 8 * 1024 * 1024) -> int:
    """Подсчитать количество строк в файле поточно (без загрузки в память)."""
    try:
        total = 0
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                total += chunk.count(b'\n')
        return total
    except Exception:
        return 0


# --------- UI ---------
st.set_page_config(page_title="csvValidator", page_icon="🧹", layout="wide")
st.title("csvValidator — просто и надёжно")

# Параметры скрыты: используем значения по умолчанию для простоты и надёжности
export_delim = "~"

# Источник данных: либо файл уже на сервере (папка data/), либо загрузка
server_path = None
try:
    server_files = sorted(list(DATA_DIR.glob("*.csv")) + list(DATA_DIR.glob("*.txt")))
except Exception:
    server_files = []
options = ["—"] + [p.name for p in server_files]
chosen = st.selectbox("Выберите файл из папки data/", options, index=0)
if chosen != "—":
    server_path = (DATA_DIR / chosen)

# Tabs for workflow
process_tab, preview_tab, logs_tab = st.tabs(["Обработка", "Просмотр данных", "Логи"])

with preview_tab:
    if server_path is None:
        st.info("Выберите файл из папки data/, чтобы увидеть предпросмотр первых строк")
    else:
        # Читаем только небольшой сэмпл для предпросмотра, не весь файл
        try:
            with open(server_path, 'rb') as f:
                sample_bytes = f.read(SAMPLE_BYTES)
        except Exception as e:
            st.error(f"Не удалось прочитать файл: {e}")
            sample_bytes = b""
        hdr, rows, det_enc, det_delim = preview_first_rows(
            sample_bytes,
            limit=200,
        )

        # Статистика по первым 10 000 строкам: колонок в заголовке и модальное число колонок
        try:
            with open(server_path, 'rb') as f:
                first_10k = read_first_n_lines_bytes(f, n=10000)
            _bio = io.BytesIO(first_10k)
            _enc = detect_encoding_and_reset(_bio)
            _bio.seek(0)
            _ts = io.TextIOWrapper(_bio, encoding=_enc, errors='ignore', newline='')
            _det_delim, _header_cols_stat, _modal_cols_stat, _stat_rows = analyze_csv_stats_stream(
                _ts, max_lines=10000, provided_delimiter=None
            )
        except Exception as _e:
            _header_cols_stat, _modal_cols_stat = (len(hdr) if hdr else 0), (len(hdr) if hdr else 0)

        colA, colB, colC = st.columns(3)
        with colA:
            st.metric("Определённая кодировка", det_enc or "—")
        with colB:
            st.metric("Определённый разделитель", repr(det_delim) if det_delim else "—")
        with colC:
            st.metric("Колонок по заголовку", (len(hdr) if hdr else _header_cols_stat))

        colD, _ = st.columns([1, 2])
        with colD:
            st.metric("Колонок статистически", _modal_cols_stat)

        if hdr:
            st.caption("Первые строки файла")
            # Show as a simple table without extra dependencies
            st.dataframe([dict(zip(hdr, r + [None] * (len(hdr) - len(r)))) for r in rows], use_container_width=True)
        else:
            st.warning("Файл пустой — нечего показывать")

with process_tab:
    if server_path is None:
        st.caption("Выберите файл для обработки…")
    else:
        st.info("Нажмите Обработать, чтобы начать очистку и конвертацию")
        if st.button("Обработать", type="primary"):
            prog = st.progress(0, text="Подготовка файла...")
            status = st.empty()

            write_log("Запуск обработки файла (CLI)")
            write_log(f"Параметры: export_delimiter={repr(export_delim)}, encoding=auto, delimiter=auto")

            try:
                # 1) Используем выбранный файл из папки data/
                input_path = server_path
                status.write(f"Используется файл на сервере: {input_path}")
                write_log(f"Используется файл на сервере: {input_path}")

                # 2) Формируем пути результатов
                stem = Path(input_path).stem
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                export_path = EXPORT_DIR / f"{stem}_clean_{ts}.csv"
                bad_path = BAD_DIR / f"{stem}_bad_{ts}.csv"
                bad_raw_path = BAD_DIR / f"{stem}_bad_{ts}_raw.txt"  # генерится скриптом автоматически

                # 3) Запускаем внешний скрипт на весь файл (с потоковым чтением stdout)
                script_path = Path(__file__).resolve().parent / "streaming_csv_parser.py"
                cmd = [
                    sys.executable,
                    str(script_path),
                    str(input_path),
                    str(export_path),
                    str(bad_path),
                    "--export_delimiter", export_delim,
                ]
                write_log(f"Команда: {' '.join(cmd)}")

                # Оценим число строк для нормального прогресс-бара (без заголовка)
                status.write("Подсчёт строк файла для оценки прогресса…")
                total_lines = count_total_lines(input_path)
                total_no_header = max(0, total_lines - 1)
                prog.progress(0.0, text=f"Запуск обработки… 0/{(total_no_header or '—')}")

                # Плейсхолдер для живых логов процесса
                live_logs = st.empty()
                tail: list[str] = []

                # Запуск процесса с построчным чтением stdout
                t0 = time.time()
                with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                ) as proc:
                    status.write("Идёт обработка, логи обновляются в реальном времени…")
                    valid_seen = 0
                    bad_seen = 0
                    valid_rows = None
                    bad_rows = None

                    assert proc.stdout is not None
                    for raw_line in proc.stdout:
                        line = raw_line.rstrip("\n")
                        if not line:
                            continue
                        # Пишем в лог сессии
                        write_log(line)
                        # Обновляем хвост логов в UI
                        tail.append(line)
                        if len(tail) > 200:
                            tail = tail[-200:]
                        live_logs.text_area("Прогресс и логи процесса", value="\n".join(tail), height=240)

                        # Парсим прогресс
                        try:
                            if "Обработано валидных строк:" in line:
                                # формат: ✅ Обработано валидных строк: 10000
                                num = int(''.join(ch for ch in line.split(':')[-1] if ch.isdigit()))
                                valid_seen = max(valid_seen, num)
                            elif "Невалидных строк:" in line:
                                num = int(''.join(ch for ch in line.split(':')[-1] if ch.isdigit()))
                                bad_seen = max(bad_seen, num)
                            elif line.strip().startswith("__SUMMARY__"):
                                parts = dict(p.split('=') for p in line.strip().split()[1:])
                                valid_rows = int(parts.get('VALID')) if 'VALID' in parts else None
                                bad_rows = int(parts.get('BAD')) if 'BAD' in parts else None
                        except Exception:
                            pass

                        processed = valid_seen + bad_seen
                        if total_no_header > 0:
                            frac = min(processed / total_no_header, 0.99)
                            prog.progress(frac, text=f"Обработка… {processed:,}/{total_no_header:,}".replace(',', ' '))
                        else:
                            # Неизвестен общий объём — имитируем плавный прогресс
                            base = (processed % 100) / 100.0
                            prog.progress(base, text=f"Обработка… обработано {processed:,} строк".replace(',', ' '))

                    ret = proc.wait()
                    dt = time.time() - t0

                if ret != 0:
                    prog.progress(0.0, text="Сбой обработки")
                    err_msg = "Ошибка обработки скриптом. Проверьте логи выше."
                    status.write(err_msg)
                    write_log(err_msg)
                    st.error(err_msg)
                else:
                    # Финальный прогресс и сводка
                    prog.progress(1.0, text="Готово")

                    # Если сводка не пришла отдельной строкой — используем наблюдаемые значения
                    if valid_rows is None:
                        valid_rows = valid_seen
                    if bad_rows is None:
                        bad_rows = bad_seen

                    summary_msg = f"Готово за {dt:.2f} сек. Результаты сохранены на диск."
                    if (valid_rows is not None) and (bad_rows is not None):
                        total_rows = valid_rows + bad_rows
                        summary_msg += f" Итог: валидных={valid_rows}, ошибок={bad_rows}, всего(без заголовка)={total_rows}."
                        write_log(f"Статистика: VALID={valid_rows}, BAD={bad_rows}, TOTAL(no header)={total_rows}")
                    status.write(summary_msg)
                    write_log(summary_msg)
                    saved_msg = f"Файлы сохранены: clean → {export_path}, bad → {bad_path}, bad_raw → {bad_raw_path}"
                    st.success("Обработка завершена успешно")
                    st.caption(saved_msg)

                    # Если есть статистика — покажем метрики
                    if (valid_rows is not None) and (bad_rows is not None):
                        m1, m2, m3 = st.columns(3)
                        with m1:
                            st.metric("Валидных строк", f"{valid_rows:,}".replace(",", " "))
                        with m2:
                            st.metric("Ошибок", f"{bad_rows:,}".replace(",", " "))
                        with m3:
                            st.metric("Всего (без заголовка)", f"{(valid_rows + bad_rows):,}".replace(",", " "))

                    # 4) Кнопки скачивания превью (первые 10 000 строк), чтобы не грузить весь файл в ОЗУ
                    preview_limit = 10000

                    def _first_n_lines_bytes(path: Path, n: int = 10000) -> bytes:
                        try:
                            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                                lines = []
                                for i, line in enumerate(f, start=1):
                                    lines.append(line)
                                    if i >= n:
                                        break
                            return ''.join(lines).encode('utf-8')
                        except Exception as e:
                            return f"Не удалось сформировать превью: {e}\n".encode('utf-8')

                    col1, col2, col3 = st.columns(3)
                    try:
                        with col1:
                            st.download_button(
                                f"Скачать превью очищенного CSV (первые {preview_limit:,} строк)".replace(',', ' '),
                                data=_first_n_lines_bytes(export_path, preview_limit),
                                file_name=f"preview_{export_path.name}",
                                mime="text/csv; charset=utf-8",
                            )
                        with col2:
                            st.download_button(
                                f"Скачать превью ошибок (первые {preview_limit:,} строк)".replace(',', ' '),
                                data=_first_n_lines_bytes(bad_path, preview_limit),
                                file_name=f"preview_{bad_path.name}",
                                mime="text/csv; charset=utf-8",
                            )
                        with col3:
                            if bad_raw_path.exists():
                                st.download_button(
                                    f"Скачать превью сырых ошибок (первые {preview_limit:,} строк)".replace(',', ' '),
                                    data=_first_n_lines_bytes(bad_raw_path, preview_limit),
                                    file_name=f"preview_{bad_raw_path.name}",
                                    mime="text/plain; charset=utf-8",
                                )
                            else:
                                st.caption("Файл сырых ошибок не найден (будет создан при наличии ошибок)")

                        st.info(
                            "Полные файлы сохранены на диск и не загружаются в память: "
                            f"clean → {export_path}, bad → {bad_path}, bad_raw → {bad_raw_path if bad_raw_path.exists() else '—'}.\n"
                            "Скачайте их напрямую с сервера/диска, если нужны целиком."
                        )
                    except Exception as save_err:
                        msg = f"Не удалось подготовить превью для скачивания: {save_err}"
                        status.write(msg)
                        write_log(msg)
            except Exception as e:
                prog.progress(0.0)
                err = f"Ошибка обработки: {e}"
                status.write(err)
                write_log(err)
                st.error(err)

with logs_tab:
    log_file = get_session_log_path()
    st.caption(f"Последние строки логов для вашей сессии: {log_file}")
    auto = st.checkbox("Автообновление каждые 5 секунд", value=True, key="logs_auto")
    log_text = read_log_tail(log_file, max_lines=500)
    st.text_area("Логи", value=log_text, height=300)
    if auto:
        time.sleep(5)
        st.rerun()
