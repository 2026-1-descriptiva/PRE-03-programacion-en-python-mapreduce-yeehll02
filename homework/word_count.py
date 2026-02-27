"""Taller evaluable"""

import glob
import os
import shutil
import string
import time


def prepate_input_dicrectory(path: str):
    """Crea la carpeta si no existe."""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    else:
        # Elimina todos los archivos en la carpeta
        for file in glob.glob(f"{path}/*"):
            os.remove(file)


# Copia los archivos raw a {input_path} usando python multi plataforma
def copy_raw_files_to_input_folder(n, raw_path, input_path):
    for file in glob.glob(f"{raw_path}/*"):
        print(f"Copying {file} to {input_path} " f" {n} times")
        with open(file, "r", encoding="utf-8") as f:
            content = f.read()
        # Crea NUMERO_COPIAS copias del archivo
        for i in range(n):
            raw_file_name = os.path.basename(file)
            raw_file_name_without_ext, raw_file_ext = os.path.splitext(raw_file_name)
            with open(
                f"{input_path}/{raw_file_name_without_ext}_{i}.{raw_file_ext.lstrip('.')}",
                "w",
                encoding="utf-8",
            ) as f:
                f.write(content)


def mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence


def reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def hadoop(mapper_func, reducer_func, input_path, output_path):
    """Simula el comportamiento de Hadoop Streaming."""

    def emit_input_lines(input_path):
        sequence = []
        files = glob.glob(f"{input_path}/*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def create_output_folder(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            raise FileExistsError(f"The folder {output_dir} already exists")

    def shuffle_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence

    def export_results_to_file(output_path, result):
        with open(f"{output_path}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_path):
        """Create Marker"""
        with open(f"{output_path}/_SUCCESS", "w", encoding="utf-8") as f:
            f.write("")  # archivo vacío

    # Lee los archivos de {input_path}
    sequence = emit_input_lines(input_path)

    # Mapea las líneas a pares (palabra, 1). Este es el mapper.
    pairs_sequence = mapper_func(sequence)

    # ordena la secuencia de pares por palabra
    pairs_sequence = shuffle_and_sort(pairs_sequence)

    # Reduce la secuencia de pares sumando los valores para cada palabra. este es el reducer.
    result = reducer_func(pairs_sequence)

    # Crea la carpeta {output_path}
    create_output_folder(output_path)

    # Guarda el resultado en un archivo {output_path}/part-00000
    export_results_to_file(output_path, result)

    # crea un archivo _SUCCESS en {output_path}
    create_success_file(output_path)


def run_job(n: int = 1000):
    """Ejecuta el job de word count."""
    raw_path = "files/raw"
    input_path = "files/input"
    output_path = "files/output"

    prepate_input_dicrectory(input_path)
    copy_raw_files_to_input_folder(n, raw_path, input_path)

    # El experimento realmente empieza en este punto.
    start_time = time.time()

    # Elimina la carpeta output/ si existe
    if os.path.exists("files/output/"):
        for file in os.listdir("files/output/"):
            file_path = os.path.join("files/output/", file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir("files/output/")

    hadoop(mapper, reducer, input_path, output_path)

    # Imprime el tiempo que tomó el experimento
    print(f"Tiempo de ejecución: {time.time() - start_time} segundos")


if __name__ == "__main__":
    run_job(n=10000)
