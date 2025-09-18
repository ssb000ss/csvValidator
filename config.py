#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Глобальная конфигурация проекта и поддержка .env.

Задача:
- Прочитать .env (если есть) из корня проекта.
- Предоставить пути до папок data/, export/, bad/ через константы DATA_DIR, EXPORT_DIR, BAD_DIR.
- Значения можно переопределить в .env переменными окружения:
    DATA_DIR=./data
    EXPORT_DIR=./export
    BAD_DIR=./bad
- Папки автоматически создаются при импорте этого модуля.

Использование:
    from config import DATA_DIR, EXPORT_DIR, BAD_DIR

Формат значений в .env может быть абсолютным или относительным. Относительные
пути интерпретируются относительно корня проекта (директория этого файла).
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    # Загрузка переменных окружения из .env в корне проекта
    from dotenv import load_dotenv  # type: ignore
except Exception:  # библиотека может быть не установлена в окружении IDE
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

# Корень проекта — папка, в которой находится этот файл
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем .env из корня проекта (если есть)
load_dotenv(PROJECT_ROOT / '.env')


def _resolve_path(value: str | os.PathLike | None, default_rel: str) -> Path:
    """Возвращает абсолютный Path. Если value не задан, берёт PROJECT_ROOT/default_rel."""
    if value:
        p = Path(value)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        return p
    return (PROJECT_ROOT / default_rel).resolve()


DATA_DIR: Path = _resolve_path(os.getenv('DATA_DIR'), 'data')
EXPORT_DIR: Path = _resolve_path(os.getenv('EXPORT_DIR'), 'export')
BAD_DIR: Path = _resolve_path(os.getenv('BAD_DIR'), 'bad')
LOGS_DIR: Path = _resolve_path(os.getenv('LOGS_DIR'), 'logs')

# Гарантируем, что директории существуют
for _d in (DATA_DIR, EXPORT_DIR, BAD_DIR, LOGS_DIR):
    try:
        _d.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Не прерываем импорт конфигурации из-за проблем с правами,
        # но оставляем возможность обработать это на уровне приложения
        pass

__all__ = ['PROJECT_ROOT', 'DATA_DIR', 'EXPORT_DIR', 'BAD_DIR', 'LOGS_DIR']
