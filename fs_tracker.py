import socket
import json
import threading
import os

# Define host and port
HOST = '0.0.0.0'  # mudar para socket.gethostname() no core
PORT = 9090

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the address and port
server_socket.bind((HOST, PORT))

# Listen for incoming connections
server_socket.listen()

print(f"Server is listening on {HOST}:{PORT}")

def process_client(node_id,client_socket):
    while True:
        data = client_socket.recv(1024)

        if not data:
            del registered_nodes[node_id]  # Remove o nó correspondente
            print(registered_nodes)
            break

        response = process_command(node_id,data)
        # print(response)
        client_socket.sendall(response)

    client_socket.close()


registered_nodes = {}
#name_to_ip = {}
files_folder = {}

def process_command(node_id,data):
    parts = data.decode('utf-8').strip().split(' ')
    command = parts[0]

    # para tirar !!!!!!!!!!!!!!!!!
    if command == 'Mapa':
        print(registered_nodes)

    if command == 'REGISTER':

        if len(parts) > 1:

            file_name = parts[1]
            blocks = parts[2:]
            print(parts)

            if node_id not in registered_nodes:
                registered_nodes[node_id] = {}  # Create an empty dictionary for the client
                # Serialize and send a response
                json_str = json.dumps(node_id + " REGISTED SUCCESSFULLY")
                bytes_data = json_str.encode('utf-8')
                response = bytes_data
            elif (node_id in registered_nodes) and (file_name not in registered_nodes[node_id]):
                json_str = json.dumps(node_id + " FILES UPDATED")
                bytes_data = json_str.encode('utf-8')
                response = bytes_data

            else:
                json_str = json.dumps(node_id + " ALREADY EXISTS")
                bytes_data = json_str.encode('utf-8')
                response = bytes_data

            if file_name not in registered_nodes[node_id]:
                registered_nodes[node_id][file_name] = blocks

            if file_name not in files_folder:
                print(blocks)
                blocks_numbrs = [int(bloco) for bloco in blocks]
                if not blocks_numbrs:
                    num_block_max = 0
                else:
                    num_block_max = max(blocks_numbrs)+1
                files_folder[file_name] = num_block_max


            elif file_name in files_folder:

                blocks_numbrs = [int(bloco) for bloco in blocks] # converte p lista de inteiros
                if not blocks_numbrs:
                    num_block_max = 0
                else:
                    num_block_max = max(blocks_numbrs)+1

                if files_folder[file_name] < num_block_max:
                    files_folder[file_name] = num_block_max

        else:
            registered_nodes[node_id] = {}  # Create an empty dictionary for the client
            # Serialize and send a response
            json_str = json.dumps(node_id + " REGISTED SUCCESSFULLY")
            bytes_data = json_str.encode('utf-8')
            response = bytes_data

        # Print the registered clients for debugging
        print(registered_nodes)
        print(files_folder)
        #print(name_to_ip)

    elif command == 'SEARCH':
        # Extract the desired file name
        if len(parts) >= 2:
            file_name = parts[1]
            response = search_file(file_name)
        else:
            # Missing file name in the command
            response = b"Missing file name in the SEARCH command"

    elif command == 'LIST_ALL_FILES':
        all_files = set()

        for client_files in registered_nodes.values():
            all_files.update(client_files)

        if all_files:
            response = f"All files in the dictionary: {', '.join(all_files)}".encode('utf-8')
        else:
            response = b"No files found in the dictionary."

    elif command == 'GET':
            if len(parts) >= 2:
                file_name = parts[1]
                response = search_file(file_name)
                json_str = json.dumps(response) # response apenas se nao funcionar e windows
                bytes_data = json_str.encode('utf-8')
                response = bytes_data

    elif command == 'EXIT':
        node_id = parts[1]  # Extrai o ID do nó do comando EXIT
        if node_id in registered_nodes:
            del registered_nodes[node_id]  # Remove o nó correspondente
            print(registered_nodes)
            response = "EXIT_SUCCESS".encode('utf-8')
        else:
            response = "Node not found".encode('utf-8')
    else:
        response = b"Unknown command"

    return response


def search_file(file_name):
    # Search for the file_name in the registered_clients dictionary
    results = []

    for node_id, client_info in registered_nodes.items():
        if file_name in client_info:
            # Append a tuple containing the node_id and blocks to results
            results.append([node_id, client_info[file_name]])
    print("files_floder: ")
    print(str(files_folder))
    print("file_name: ")
    print(file_name)

    results_aux = [results, files_folder[file_name]]

    print("resultado: " + str(results))
    print("results_aux: " + str(results_aux))

    if not results:
        results = None

    return results_aux


# Accept connections in a loop and create a new thread for each client
while True:
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")

    # Start a new thread to handle the client
    client_thread = threading.Thread(target=process_client, args=(client_address[0], client_socket,))
    client_thread.start()

