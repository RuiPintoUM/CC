import json
import socket
import sys
import os
import threading
import time
import ast
import random
import hashlib

# Check if the required command-line arguments are provided
if len(sys.argv) < 4:
    print("Usage: python fs_node.py <HOST> <PORT>")
    sys.exit(1)

# Get host and port from command-line arguments
shared_folder = sys.argv[1]
HOST = sys.argv[2]
PORT = int(sys.argv[3])  # Convert port to an integer
PORT_UDP_SERVER = int(sys.argv[4])

# Create a socket object
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
client_socket.connect((HOST, PORT))

def calculate_checksum(data):
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.digest()

def list_files_in_shared_folder(shared_folder):
    try:
        files = os.listdir(shared_folder)

        if not files:
                print(f"No files found in '{shared_folder}'. Sending a REGISTER command with no files.")

                command = "REGISTER"
                print("Generated Command:", command)

                # Send the command to the server
                client_socket.send(command.encode('utf-8'))
                response = client_socket.recv(1024)
                print(response.decode('utf-8'))

        else:
            for filename in files:
                file_path = os.path.join(shared_folder, filename)

                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    print(f"File: {filename}, Size: {file_size} bytes")
                    # Split the file into blocks
                    blocks = [str(b) for b in range((file_size + 1023) // 1024)]
                    #Cria pasta com blocos dos ficheiros
                    print("antes de dividir em blocos")
                    split_text_file(file_path, shared_folder)
                    print("depois de dividir em blocos")
                    # Generate the REGISTER command
                    command = f"REGISTER {filename} {' '.join(blocks)}"
                    print("Generated Command:", command)
                    # Send the command to the server
                    client_socket.send(command.encode('utf-8'))
                    response = client_socket.recv(1024)
                    print(response.decode('utf-8'))

    except FileNotFoundError:
        print(f"Shared folder '{shared_folder}' not found.")
    except PermissionError:
        print(f"Permission denied while accessing '{shared_folder}'.")


def split_text_file(input_file_path, output_folder_path):
    # Verifica se o arquivo de entrada existe
    if not os.path.exists(input_file_path):
        print(f"O arquivo {input_file_path} não existe.")
        return

    #Obtém o nome do arquivo sem extensão
    file_name = os.path.splitext(os.path.basename(input_file_path))[0]

    #Cria a diretoria de saída
    output_folder = os.path.join(output_folder_path, file_name)
    os.makedirs(output_folder, exist_ok=True)

    block_size = 1024

    #abre o arquivo de entrada para leitura binária
    with open(input_file_path, 'rb') as file:
        block_number = 0
        while True:
            #Lê o bloco de dados
            block_data = file.read(block_size)
            if not block_data:  # Verifica se há dados para ler
                break

            #Cria o caminho do arquivo de saída para o bloco
            output_file_path = os.path.join(output_folder, f'{file_name}_block{block_number}.txt')

            #Abre o arquivo de saida para escrita binária
            with open(output_file_path, 'wb') as output_file:
                output_file.write(block_data)

            block_number += 1

    print(f"A divisão do arquivo {input_file_path} foi concluída. Os blocos estão em {output_folder}.")

def periodic_list_files():
    while True:
        list_files_in_shared_folder(shared_folder)
        time.sleep(180)  # Execute every 180 seconds


def selection(nodes_blocks_list):
    my_array = ast.literal_eval(nodes_blocks_list)  #[[[ip,blocks,3],...],n blocks max]
    print("informação recebida: ")
    print(my_array)

    solution = []
    total_blocks = my_array[1]
    list_of_nodes = my_array[0]
    number_of_nodes = len(list_of_nodes)

    chosen_node = random.randint(0, number_of_nodes - 1)

    while len(list_of_nodes[chosen_node][1]) < total_blocks:
        chosen_node = random.randint(0, number_of_nodes - 1)

    solution.append(list_of_nodes[chosen_node])

    if len(list_of_nodes) > 1:
        list_of_nodes.remove(list_of_nodes[chosen_node])
        chosen_node2 = random.randint(0, number_of_nodes - 2)

        while len(list_of_nodes[chosen_node2][1]) < total_blocks:
            chosen_node2 = random.randint(0, number_of_nodes - 2)

        solution.append(list_of_nodes[chosen_node2])

    print(solution)

    return solution

def deal_node_server_task(server_socket,actual_message,client_address):


    if actual_message[0] == "REQUEST":
        #print("reparei que era request\n")
        filename = actual_message[2]
        filepath = os.path.join(os.getcwd(), shared_folder, actual_message[1],filename)

        try:
            with open(filepath, 'r') as file:
                conteudo = file.read()
                checksum = calculate_checksum(conteudo.encode('utf-8'))
                data_to_send = {
                    'checksum' : checksum,
                    'content' : conteudo
                }
                serialized_data = str(data_to_send).encode('utf-8')

            bytes_sent = server_socket.sendto(serialized_data, (client_address[0], PORT_UDP_SERVER))

            #print(f"Conteudo prestes a ser enviado via socket UDP: {conteudo}\n")

            info, addr = server_socket.recvfrom(1024)

            if info.decode('utf-8') == 'ACK':
                print("Recebi ACK, bloco transferido sem problema de checksum")
            else:
                print("Houve problema de checksum a enviar bloco outra vez")
                server_socket.sendto(serialized_data, addr)

            print(f"Enviado\n")

        except FileNotFoundError:
            print(f"O arquivo não foi encontrado.")



def udp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    PORT_UDP_SERVER_AUX = PORT_UDP_SERVER + 1

    server_address = ("0.0.0.0", PORT_UDP_SERVER_AUX)
    server_socket.bind(server_address) # criar antes e enviar

    print(f"Listening UDP on {server_address}")

    while True:
        data, client_address = server_socket.recvfrom(1024) # ajustar tamanho

        #print("mesage: \n")
        message = data.decode('utf-8')
        print(f"o que vai ser convertido num array: {message}")
        actual_message = string_para_array(message)

        print(data)

        thread = threading.Thread(target=deal_node_server_task, args=(server_socket,actual_message,client_address))
        thread.daemon = True
        thread.start()

        print("thread for task started")


def string_para_array(string):
    try:
        resultado = ast.literal_eval(string)
        return resultado
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None

def string_para_dicionario(input_string):
    try:
        # Utiliza a função ast.literal_eval para converter a string num dicionário
        result_dict = ast.literal_eval(input_string)
        if isinstance(result_dict, dict):
            return result_dict
        else:
            raise ValueError("A string não representa um dicionário válido.")
    except (ValueError, SyntaxError) as e:
        print(f"Erro ao converter a string para dicionário: {e}")
        return None


def juntar_blocos_num_arquivo(path_diretoria_entrada, path_arquivo_saida):
    if not os.path.exists(path_diretoria_entrada):
        # print(f"A diretoria "{path_diretoria_entrada}" não existe.")
        return

    arquivos = os.listdir(path_diretoria_entrada)

    with open(path_arquivo_saida, 'w') as arquivo_saida:
        #Itera sobre cada bloco na diretoria
        for nome_bloco in arquivos:
            caminho_bloco = os.path.join(path_diretoria_entrada, nome_bloco)
            if os.path.isfile(caminho_bloco):
                with open(caminho_bloco, 'r') as arquivo_entrada:
                    content = arquivo_entrada.read()

                    arquivo_saida.write(content)

def get_Node_Blocks(udp_client_socket,node, start_block, final_block,filename,path_for_receiving_blocks):

    i = start_block

    while i < final_block:

        comando = str(["REQUEST", filename, f"{filename}_block{i}.txt"])

        PORT_UDP_SERVER_AUX = PORT_UDP_SERVER + 1

        bytes_sent = udp_client_socket.sendto(comando.encode('utf-8'), (node, PORT_UDP_SERVER_AUX))

        os.makedirs(path_for_receiving_blocks, exist_ok=True)

        with open(os.path.join(path_for_receiving_blocks, f"{filename}_block{i}.txt"), 'w') as file:

            data = str

            try:
                data, addr = udp_client_socket.recvfrom(2048)
                real_data = string_para_dicionario(data.decode('utf-8'))
                content = real_data['content']
                checksum_recv = real_data['checksum']
                checksum2 = calculate_checksum(content.encode('utf-8')) # Checksum do conteudo recebido para comparação
                if checksum_recv == checksum2:
                    file.write(content)
                    ack = "ACK"
                    udp_client_socket.sendto(ack.encode('utf-8'), (addr))


            except TimeoutError:
                print("TimeoutError: não foram recebidos mais dados, encerrando a transmissão.")
                continue

        i += 1


periodic_thread = threading.Thread(target=periodic_list_files)
periodic_thread.daemon = True
periodic_thread.start()

server_udp_thread = threading.Thread(target=udp_server)
server_udp_thread.daemon = True
server_udp_thread.start()


while True:
    command = input("Enter a command (e.g., REGISTER Node1 file1 1,2,3): ")
    command_div = command.split()

    if command == "EXIT":
        command += " " + HOST
        client_socket.send(command.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        if response == "EXIT_SUCCESS":
            break
    if command_div[0] == "GET":
        filename = command_div[1]
        client_socket.send(command.encode('utf-8'))
        response = client_socket.recv(1024)
        node_info = json.loads(response)

        str_response = response.decode('utf-8')

        solution = selection(response.decode('utf-8')) # solution a dar lista [[N1],[N2]]

        actual_response = string_para_array(str_response)
        print("actual_response: " + str(actual_response))

        number_of_blocks = actual_response[1]

        udp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_client_socket.bind(("0.0.0.0", PORT_UDP_SERVER))

        seconds_for_timeout = 15
        udp_client_socket.settimeout(seconds_for_timeout)

        path_for_receiving_blocks = os.path.join(shared_folder, filename.split(".")[0])

        file_without_extension = filename.split(".")[0]

        blocks_per_Node = number_of_blocks // 2
        start_block = 0
        final_block = blocks_per_Node
        node_on_the_list = 0
        threads = []

        # Diferenciar no caso de só haver um Node com o File ou haver 2
        if (len(solution ) < 2):
            thread = threading.Thread(target=get_Node_Blocks, args=(udp_client_socket, solution[node_on_the_list][0], start_block, number_of_blocks, file_without_extension, path_for_receiving_blocks))
            thread.daemon = True
            thread.start()

            thread.join()

        else:
            while(node_on_the_list < len(solution)):

                print(node_on_the_list)

                thread = threading.Thread(target=get_Node_Blocks, args=(udp_client_socket, solution[node_on_the_list][0], start_block, final_block, file_without_extension, path_for_receiving_blocks))
                thread.daemon = True
                thread.start()

                threads.append(thread)

                node_on_the_list += 1

                start_block += blocks_per_Node

                if number_of_blocks % 2 == 0:
                    final_block += blocks_per_Node
                else:
                    final_block += blocks_per_Node + 1

            for thread in threads:
                thread.join()

        juntar_blocos_num_arquivo(path_for_receiving_blocks, os.path.join(shared_folder, filename))
        list_files_in_shared_folder(shared_folder)

        udp_client_socket.close()

    else:
        client_socket.send(command.encode('utf-8'))

        response = client_socket.recv(1024)
        print(response.decode('utf-8'))
