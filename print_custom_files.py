import os

def print_custom_files(location):
    # Cria uma lista vazia para armazenar os nomes dos arquivos
    files = []

    # Loop pelos arquivos no diretório especificado pela variável "location"
    for filename in os.listdir(location):
        # Verifica se o nome do arquivo termina com ".flow"
        if filename.endswith(".flow"):
            # Se o arquivo termina com ".flow", adiciona o nome do arquivo à lista "files"
            files.append(filename)

    # Retorna a lista de arquivos que terminam com ".flow"
    return files

