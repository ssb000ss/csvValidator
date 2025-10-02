#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
streaming_csv_parser

CLI-инструмент для потоковой обработки CSV: автоопределение кодировки и
разделителя, статистический анализ структуры, правило 90% для выбора ожидаемого
количества колонок и обработка строк с попыткой склейки.

Запуск:
    python streaming_csv_parser.py input.csv export.csv bad.csv [--encoding ...]

См. README.md для подробностей.
"""

import argparse
import csv
import chardet
import logging
from pathlib import Path
from typing import Generator, Tuple

# Настройка логгера
from logging.handlers import RotatingFileHandler
from config import LOGS_DIR

logger = logging.getLogger("streaming_csv_parser")
logger.setLevel(logging.INFO)

# Создаём обработчики: ротация файлов в общем каталоге логов + консоль
log_file = (LOGS_DIR / 'streaming_csv_parser.log')
file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
console_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Избежать дублирования хэндлеров при повторном импорте
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def detect_encoding(file_path: Path, sample_size: int = 10000) -> str:
    """Определяет кодировку файла."""
    try:
        with open(file_path, "rb") as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'utf-8'

            if encoding.lower() == 'ascii':
                encoding = 'utf-8'

            logger.info(f"✅ Определена кодировка: {encoding}")
            return encoding
    except Exception as e:
        logger.warning(f"⚠️ Ошибка определения кодировки: {e}. Используем utf-8")
        return 'utf-8'


def detect_delimiter(file_path: Path, encoding: str, max_lines: int = 30) -> str:
    """Определяет разделитель CSV."""
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            lines = [f.readline() for _ in range(max_lines)]
            sample = ''.join(lines)

            # Используем встроенный CSV sniffer
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
                logger.info(f"✅ Определен разделитель: {repr(delimiter)}")
                return delimiter
            except csv.Error:
                # Fallback на частотный анализ
                delimiters = [',', ';', '\t', '|']
                counts = {d: sum(line.count(d) for line in lines) for d in delimiters}
                delimiter = max(counts, key=counts.get)
                logger.info(f"✅ Определен разделитель (частотный анализ): {repr(delimiter)}")
                return delimiter
    except Exception as e:
        logger.warning(f"⚠️ Ошибка определения разделителя: {e}. Используем запятую")
        return ','


def analyze_csv_stats(file_path: Path, encoding: str, max_lines: int = 10000, provided_delimiter: str | None = None):
    """
    Статистически анализирует первые max_lines строк, подбирая разделитель и структуру.
    Возвращает: (delimiter, header_cols, modal_cols, total_rows, modal_share)
    Алгоритм:
    - Кандидаты: переданный delimiter, затем результат csv.Sniffer(), затем [',',';','\t','|'].
    - Вычисляем модальное число колонок и долю модального для каждого кандидата.
    - Предпочитаем долю выше, затем большее число колонок (>1), затем модальное количество строк.
    - Если все кандидаты дают 1 колонку, используем частотный анализ (среднее число вхождений разделителя) и
      повторно считаем статистику для лучшего по частоте кандидата.
    """
    import io as _io
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore', newline='') as f:
            lines = []
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
        if not lines:
            raise ValueError("Файл пустой — нет строк для анализа")

        # Подготовим кандидатов: предоставленный, затем sniffer, затем стандартные
        base_candidates = [',', ';', '\t', '|']
        candidates: list[str] = []
        if provided_delimiter:
            candidates.append(provided_delimiter)
        # Попробуем sniffer
        try:
            sniffed = csv.Sniffer().sniff(''.join(lines)).delimiter
            if sniffed and sniffed not in candidates:
                candidates.append(sniffed)
        except Exception:
            pass
        # Добавим базовые
        for d in base_candidates:
            if d not in candidates:
                candidates.append(d)

        results = []  # (modal_share, modal_cols, modal_count, delim, header_cols, total_rows)
        freq_counts = {}  # среднее число вхождений delimeter в строке
        sample_text = ''.join(lines)
        for d in candidates:
            try:
                # Частотные характеристики
                if lines:
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
                    modal_share = modal_count / max(1, len(data_lengths))
                else:
                    modal_cols, modal_count, modal_share = header_cols, 0, 1.0
                total_rows_local = len(lengths)
                results.append((modal_share, modal_cols, modal_count, d, header_cols, total_rows_local))
            except Exception:
                continue
        if not results:
            # Фолбэк: попробуем выбрать разделитель по частоте вхождений и вернуть консервативные оценки
            try:
                if lines:
                    # Обновим freq_counts если пустой
                    if not freq_counts:
                        for d in candidates:
                            try:
                                freq_counts[d] = sum(line.count(d) for line in lines) / max(1, len(lines))
                            except Exception:
                                freq_counts[d] = 0.0
                    # Выбираем разделитель с максимальной средней частотой
                    best_d = max(freq_counts.items(), key=lambda kv: kv[1])[0] if freq_counts else ','
                    # Пытаемся распарсить ещё раз аккуратно
                    try:
                        reader = csv.reader(_io.StringIO(sample_text), delimiter=best_d, quoting=csv.QUOTE_MINIMAL)
                        lengths = [len(row) for row in reader]
                    except Exception:
                        # Грубая оценка: по split
                        lengths = [len(line.split(best_d)) for line in lines]
                    if not lengths:
                        lengths = [1]
                    header_cols = lengths[0]
                    data_lengths = lengths[1:] if len(lengths) > 1 else []
                    if data_lengths:
                        from collections import Counter
                        cnt = Counter(data_lengths)
                        modal_cols, modal_count = max(cnt.items(), key=lambda kv: (kv[1], kv[0]))
                        modal_share = modal_count / max(1, len(data_lengths))
                    else:
                        modal_cols, modal_count, modal_share = header_cols, 0, 1.0
                    total_rows_local = len(lengths)
                    logger.warning("⚠️ Статистика не определена стандартным способом, используем частотный фолбэк")
                    return best_d, header_cols, modal_cols, total_rows_local, modal_share
                else:
                    # Вообще нет строк — файл пустой
                    logger.warning("⚠️ Пустой файл при анализе статистики; возвращаем консервативные значения")
                    return ',', 0, 0, 0, 1.0
            except Exception as _e:
                # В случае любой неожиданной ошибки — вернуть безопасные значения
                logger.warning(f"⚠️ Фолбэк статистики завершился с ошибкой: {_e}. Возвращаем значения по умолчанию")
                return ',', 1, 1, len(lines), 1.0

        # Если среди кандидатов есть modal_cols > 1 — игнорируем варианты с 1 колонкой
        max_cols = max(r[1] for r in results)
        if max_cols > 1:
            filtered = [r for r in results if r[1] > 1]
        else:
            filtered = results

        # Сортируем по (доля, число колонок, количество строк с модальным)
        filtered.sort(key=lambda r: (r[0], r[1], r[2]), reverse=True)
        best = filtered[0]

        # Если лучший вариант всё ещё даёт 1 колонку, применим частотный фоллбек
        if best[1] == 1:
            # Выберем разделитель с наибольшей средней частотой в строке
            if freq_counts:
                best_d = max(freq_counts.items(), key=lambda kv: kv[1])[0]
                # Пересчитаем статистику для best_d
                reader = csv.reader(_io.StringIO(sample_text), delimiter=best_d, quoting=csv.QUOTE_MINIMAL)
                lengths = [len(row) for row in reader]
                header_cols = lengths[0] if lengths else 1
                data_lengths = lengths[1:] if len(lengths) > 1 else []
                if data_lengths:
                    from collections import Counter
                    cnt = Counter(data_lengths)
                    modal_cols, modal_count = max(cnt.items(), key=lambda kv: (kv[1], kv[0]))
                    modal_share = modal_count / max(1, len(data_lengths))
                else:
                    modal_cols, modal_count, modal_share = header_cols, 0, 1.0
                total_rows_local = len(lengths)
                best = (modal_share, modal_cols, modal_count, best_d, header_cols, total_rows_local)

        best_share, best_modal_cols, _best_modal_count, best_delim, best_header_cols, best_total_rows = best
        return best_delim, best_header_cols, best_modal_cols, best_total_rows, best_share
    except Exception as e:
        raise ValueError(f"Ошибка статистического анализа CSV: {e}")


def process_csv_streaming(input_path: Path, output_path: Path, bad_path: Path,
                          encoding: str, delimiter: str, export_delimiter: str = '~',
                          batch_size: int = 10000, expected_columns=None, bad_raw_path: Path | None = None):
    """Обрабатывает CSV потоково для экономии памяти."""

    valid_count = 0
    bad_count = 0

    try:
        # Определим путь для файла сырых ошибок (линии без изменений)
        if bad_raw_path is None:
            bad_raw_path = bad_path.with_name(f"{bad_path.stem}_raw.txt")
        with open(input_path, 'r', encoding=encoding, errors='ignore', newline='') as infile, \
                open(output_path, 'w', encoding='utf-8', newline='') as outfile, \
                open(bad_path, 'w', encoding='utf-8', newline='') as badfile, \
                open(bad_raw_path, 'w', encoding='utf-8', newline='') as badraw:

            # Создаем CSV reader с исходным разделителем
            reader = csv.reader(infile, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)

            # Создаем CSV writer с новым разделителем и правильным экранированием
            writer = csv.writer(outfile, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)
            bad_writer = csv.writer(badfile, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)

            # Записываем заголовок в bad файл с описанием колонок
            bad_header = ['Номер_строки', 'Тип_ошибки', 'Описание_ошибки', 'Содержимое_строки']
            bad_writer.writerow(bad_header)

            # Обрабатываем заголовок
            try:
                header = next(reader)
                writer.writerow(header)
                header_cols = len(header)
                if expected_columns is None:
                    expected_columns_local = header_cols
                else:
                    expected_columns_local = expected_columns
                logger.info(f"📋 Заголовок: {header_cols} столбцов; используем expected_columns={expected_columns_local}")
                logger.info(f"🔧 Новый разделитель: {repr(export_delimiter)}")
            except StopIteration:
                logger.error("❌ Файл пустой или не содержит заголовок")
                return

            # Обрабатываем строки потоково с возможностью склейки двух последовательных строк
            import io as _io

            def _parse_row(text: str):
                try:
                    return next(csv.reader(_io.StringIO(text), delimiter=delimiter, quoting=csv.QUOTE_MINIMAL))
                except StopIteration:
                    return []
                except Exception:
                    return None

            phys_line_no = 2
            buffered_line = None
            while True:
                try:
                    if buffered_line is not None:
                        cur_line = buffered_line
                        buffered_line = None
                        from_buffer = True
                    else:
                        cur_line = infile.readline()
                        from_buffer = False
                    if not cur_line:
                        break

                    row = _parse_row(cur_line)
                    if row is not None and len(row) == expected_columns_local:
                        # Валидная строка — сразу пишем
                        writer.writerow(row)
                        valid_count += 1
                        if valid_count % batch_size == 0:
                            logger.info(f"✅ Обработано валидных строк: {valid_count}")
                        phys_line_no += 1
                        continue

                    # Невалидная — пробуем склеить с одной следующей строкой
                    next_line = infile.readline()
                    if next_line:
                        combined = cur_line.rstrip('\r\n') + next_line.lstrip('\r\n')
                        combined_row = _parse_row(combined)
                        if combined_row is not None and len(combined_row) == expected_columns_local:
                            writer.writerow(combined_row)
                            valid_count += 1
                            if valid_count % batch_size == 0:
                                logger.info(f"✅ Обработано валидных строк: {valid_count}")
                            # потребили две строки
                            phys_line_no += 2
                        else:
                            # Склейка не помогла — записываем в bad только текущую строку,
                            # следующую вернём в буфер для самостоятельной обработки
                            bad_len = (len(row) if row is not None else '—')
                            content = cur_line.rstrip('\r\n')
                            if content.strip() == "":
                                desc = "Пустая строка"
                            else:
                                desc = (
                                    f"Неверное количество столбцов: {bad_len} вместо {expected_columns_local}"
                                    if isinstance(bad_len, int) else f"Неверная строка (ожидалось {expected_columns_local} столбцов)"
                                )
                            bad_writer.writerow([phys_line_no, "Ошибка_структуры", desc, content])
                            # В файл сырых ошибок пишем строку без изменений
                            badraw.write(cur_line)
                            bad_count += 1
                            if bad_count % 1000 == 0:
                                logger.warning(f"⚠️ Невалидных строк: {bad_count}")
                            phys_line_no += 1
                            buffered_line = next_line
                    else:
                        # нет следующей строки — фиксируем текущую как битую
                        content = cur_line.rstrip('\r\n')
                        if content.strip() == "":
                            desc = "Пустая строка"
                        else:
                            bad_len = (len(row) if row is not None else '—')
                            desc = (
                                f"Неверное количество столбцов: {bad_len} вместо {expected_columns_local}"
                                if isinstance(bad_len, int) else f"Неверная строка (ожидалось {expected_columns_local} столбцов)"
                            )
                        bad_writer.writerow([phys_line_no, "Ошибка_структуры", desc, content])
                        # В файл сырых ошибок пишем строку без изменений
                        badraw.write(cur_line)
                        bad_count += 1
                        if bad_count % 1000 == 0:
                            logger.warning(f"⚠️ Невалидных строк: {bad_count}")
                        phys_line_no += 1

                except Exception as e:
                    # Ошибка при обработке
                    error_desc = f"Ошибка обработки: {str(e)[:100]}"
                    bad_writer.writerow([phys_line_no, "Ошибка_обработки", error_desc, (cur_line.rstrip('\r\n') if 'cur_line' in locals() else '')])
                    # В файл сырых ошибок пишем исходную строку, если она есть
                    if 'cur_line' in locals():
                        badraw.write(cur_line)
                    bad_count += 1
                    if bad_count % 1000 == 0:
                        logger.warning(f"⚠️ Невалидных строк: {bad_count}")
                    phys_line_no += 1

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

    logger.info(f"✅ Обработка завершена. Валидных: {valid_count}, Невалидных: {bad_count}")
    return valid_count, bad_count


def main():
    parser = argparse.ArgumentParser(description='Потоковый CSV парсер для больших файлов с редким разделителем')
    parser.add_argument('input_path', help='Путь к входному CSV файлу')
    parser.add_argument('output_path', help='Путь для сохранения очищенного CSV')
    parser.add_argument('bad_path', help='Путь для сохранения невалидных строк')
    parser.add_argument('--encoding', help='Кодировка файла (автоопределение по умолчанию)')
    parser.add_argument('--delimiter', help='Разделитель входного CSV (автоопределение по умолчанию)')
    parser.add_argument('--export_delimiter', default='~', help='Разделитель для выходного файла (по умолчанию: ~)')
    parser.add_argument('--sample_size', type=int, default=10000,
                        help='Размер выборки для определения кодировки')
    parser.add_argument('--batch_size', type=int, default=10000,
                        help='Размер батча для логирования прогресса')

    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    bad_path = Path(args.bad_path)

    if not input_path.exists():
        logger.error(f"❌ Входной файл не найден: {input_path}")
        return

    try:
        # Определяем параметры файла
        encoding = args.encoding or detect_encoding(input_path, args.sample_size)
        # Статистический анализ по первым 10000 строкам с приоритетом >1 колонка
        delim, header_cols, modal_cols, total_rows, modal_share = analyze_csv_stats(
            input_path, encoding, max_lines=10000, provided_delimiter=args.delimiter
        )
        export_delimiter = args.export_delimiter

        logger.info(f"🔍 Параметры: encoding={encoding}, входной разделитель={repr(delim)}")
        logger.info(f"📊 Статистика по первым {total_rows} строкам: header_cols={header_cols}, modal_cols={modal_cols}, modal_share={modal_share:.3f}")
        logger.info(f"🔧 Выходной разделитель: {repr(export_delimiter)}")
        logger.info(f"📁 Размер файла: {input_path.stat().st_size / (1024 * 1024):.1f} MB")

        # Правило 90%: если >= 0.9 строк соответствуют modal_cols, продолжаем без ошибки
        expected_columns = modal_cols if modal_share >= 0.9 else header_cols
        if header_cols != modal_cols and modal_share < 0.9:
            logger.warning(
                f"⚠️ Заголовок: {header_cols} колонок, статистически: {modal_cols} (доля {modal_share:.2%}). Продолжаем с expected_columns={expected_columns}."
            )

        # Обрабатываем CSV
        valid_count, bad_count = process_csv_streaming(
            input_path, output_path, bad_path, encoding, delim, export_delimiter, args.batch_size, expected_columns=expected_columns
        )

        # Выведем краткую сводку в stdout в стабильном формате для UI
        try:
            print(f"__SUMMARY__ VALID={valid_count} BAD={bad_count}")
        except Exception:
            pass
        logger.info("✅ Обработка CSV завершена успешно!")

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
