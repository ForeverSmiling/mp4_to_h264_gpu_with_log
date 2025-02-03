"""
Video Compression Script v4.0
- Поддержка GPU ускорения
- Интеллектуальный пропуск файлов
- Двойной прогресс-бар
- Подробное логгирование
"""

import os
import subprocess
import time
import re
import sys
import csv
from datetime import timedelta
from tqdm import tqdm

# ==============================================================================
# КОНФИГУРАЦИЯ СИСТЕМЫ
# ==============================================================================
GPU_CONFIG = {
    'nvidia': {
        'encoder': 'h264_nvenc',
        'preset': 'p6',
        'crf_param': '-cq:v',
        'extra_params': ['-rc:v', 'constqp']
    },
    'amd': {
        'encoder': 'h264_amf', 
        'preset': 'speed',
        'crf_param': '-qp_i'
    },
    'intel': {
        'encoder': 'h264_qsv',
        'preset': 'faster',
        'crf_param': '-global_quality'
    },
    'cpu': {
        'encoder': 'libx264',
        'preset': 'fast',
        'crf_param': '-crf'
    }
}

# ==============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================================================================

def init_folders():
    """Инициализирует рабочие директории"""
    required_folders = ['compressed', 'skipped']
    for folder in required_folders:
        os.makedirs(folder, exist_ok=True)

def init_log_file():
    """Создает файл лога с заголовками"""
    if not os.path.exists('processing_log.csv'):
        with open('processing_log.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Filename',
                'Original Size (MB)',
                'Compressed Size (MB)',
                'Compression Ratio (%)',
                'Skipped'
            ])

def log_to_csv(filename, orig_size, compr_size, skipped):
    """Записывает результат обработки в CSV"""
    try:
        compression_ratio = 100 - (compr_size/orig_size)*100 if orig_size > 0 else 0
        with open('processing_log.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                filename,
                round(orig_size, 2),
                round(compr_size, 2),
                round(compression_ratio, 2),
                int(skipped)
            ])
    except Exception as e:
        print(f"Ошибка записи в лог: {str(e)}")

def parse_time(time_str):
    """Парсит строку времени из FFmpeg в секунды"""
    try:
        parts = re.split(r'[:.]', time_str)
        if len(parts) == 3:  # MM:SS.ss
            return 0, int(parts[0]), float(parts[1])
        elif len(parts) >= 4:  # HH:MM:SS.ss
            return int(parts[0]), int(parts[1]), float(parts[2])
    except Exception as e:
        tqdm.write(f"Ошибка парсинга времени: {str(e)}")
    return 0, 0, 0

def get_gpu_type():
    """Определяет доступное аппаратное ускорение"""
    try:
        result = subprocess.run(['ffmpeg', '-encoders'], 
                              capture_output=True, 
                              text=True,
                              check=True)
        encoders = result.stdout.lower()
        if 'nvenc' in encoders:
            return 'nvidia'
        elif 'amf' in encoders:
            return 'amd'
        elif 'qsv' in encoders:
            return 'intel'
    except Exception as e:
        tqdm.write(f"Ошибка определения GPU: {str(e)}")
    return 'cpu'

# ==============================================================================
# ОСНОВНАЯ ФУНКЦИЯ СЖАТИЯ
# ==============================================================================

def compress_video(input_path, output_folder, gpu_type, crf=23, position=1):
    """
    Выполняет сжатие видео с проверкой результатов
    Возвращает: (original_size, compressed_size, skipped)
    """
    skipped = False
    filename = os.path.basename(input_path)
    output_path = os.path.join(output_folder, f"compressed_{filename}")
    
    try:
        # Проверка существования исходного файла
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Файл {input_path} не найден")

        # Пропуск уже обработанных файлов
        if os.path.exists(os.path.join('skipped', filename)):
            return 0, 0, True

        original_size = os.path.getsize(input_path) / (1024 ** 2)  # MB

        # Конфигурация кодировщика
        config = GPU_CONFIG[gpu_type]
        cmd = [
            'ffmpeg',
            '-hide_banner',
            '-y',
            '-hwaccel', 'auto' if gpu_type != 'cpu' else 'none',
            '-i', input_path,
            '-c:v', config['encoder'],
            '-preset', config['preset'],
            config['crf_param'], str(crf),
            '-c:a', 'copy',
            '-movflags', '+faststart',
            output_path
        ]
        if 'extra_params' in config:
            cmd.extend(config['extra_params'])

        # Запуск процесса кодирования
        process = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
            encoding='utf-8'
        )

        # Определение длительности видео
        duration = None
        while True:
            line = process.stderr.readline()
            if 'Duration:' in line:
                time_str = line.split('Duration: ')[1].split(',')[0].strip()
                h, m, s = parse_time(time_str)
                duration = timedelta(hours=h, minutes=m, seconds=s).total_seconds()
                break

        # Прогресс-бар для текущего файла
        with tqdm(total=duration,
                 desc=filename[:20].ljust(20),
                 unit='s',
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}',
                 position=position,
                 leave=False) as pbar_file:
            
            while True:
                line = process.stderr.readline()
                if not line and process.poll() is not None:
                    break
                if 'time=' in line:
                    time_str = line.split('time=')[1].split()[0]
                    h, m, s = parse_time(time_str)
                    current_time = timedelta(hours=h, minutes=m, seconds=s).total_seconds()
                    pbar_file.n = current_time
                    pbar_file.refresh()

        # Проверка результата
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg ошибка: код {process.returncode}")

        compressed_size = os.path.getsize(output_path) / (1024 ** 2)

        # Проверка эффективности сжатия
        if compressed_size >= original_size:
            os.remove(output_path)
            os.rename(input_path, os.path.join('skipped', filename))
            skipped = True
            return original_size, original_size, skipped

        return original_size, compressed_size, skipped

    except Exception as e:
        # Очистка при ошибках
        if os.path.exists(output_path):
            os.remove(output_path)
        if os.path.exists(input_path):
            os.rename(input_path, os.path.join('skipped', filename))
        return original_size if 'original_size' in locals() else 0, 0, True

# ==============================================================================
# УПРАВЛЕНИЕ ПРОЦЕССОМ ОБРАБОТКИ
# ==============================================================================

def main():
    """Основная функция управления обработкой"""
    init_folders()
    init_log_file()
    
    crf = 23
    input_folder = '.'
    gpu_type = get_gpu_type()
    
    # Получение списка файлов
    files = [f for f in os.listdir(input_folder) 
            if f.lower().endswith('.mp4') 
            and not f.startswith('compressed_')]
    
    if not files:
        tqdm.write("Нет файлов для обработки!")
        return

    total_files = len(files)
    start_time = time.time()
    
    # Статистика выполнения
    tqdm.write(f"Начата обработка {total_files} файлов")
    tqdm.write(f"Используемое ускорение: {gpu_type.upper()}")
    tqdm.write("=" * 50 + "\n")

    # Основной цикл обработки
    with tqdm(total=total_files, 
             desc="Общий прогресс".ljust(20),
             bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}',
             position=0) as pbar_total:
        
        for file in files:
            file_start = time.time()
            input_path = os.path.join(input_folder, file)
            skipped = False
            
            try:
                # Пропуск уже перемещенных файлов
                if os.path.exists(os.path.join('skipped', file)):
                    pbar_total.update(1)
                    continue

                # Обработка файла
                orig_size, compr_size, skipped = compress_video(
                    input_path, 'compressed', gpu_type, crf, position=1
                )

                # Логирование результатов
                log_to_csv(file, orig_size, compr_size, skipped)

                # Вывод статистики
                if skipped:
                    tqdm.write(f"[ПРОПУЩЕНО] {file} - сжатие неэффективно")
                else:
                    ratio = 100 - (compr_size/orig_size)*100
                    tqdm.write(
                        f"[УСПЕШНО] {file} "
                        f"({orig_size:.2f}MB → {compr_size:.2f}MB, "
                        f"-{ratio:.1f}%)"
                    )

            except Exception as e:
                tqdm.write(f"[ОШИБКА] {file} - {str(e)}")
                log_to_csv(file, 0, 0, True)
                
            finally:
                pbar_total.update(1)

    # Финал выполнения
    total_time = time.time() - start_time
    tqdm.write("\n" + "=" * 50)
    tqdm.write(f"Обработка завершена за {timedelta(seconds=total_time)}")
    tqdm.write(f"Результаты:")
    tqdm.write(f"- Сжатые файлы: ./compressed")
    tqdm.write(f"- Пропущенные файлы: ./skipped")
    tqdm.write(f"- Детальный лог: processing_log.csv")

if __name__ == "__main__":
    main()