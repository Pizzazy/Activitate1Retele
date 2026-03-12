import socket
import threading

HOST = "127.0.0.1"
PORT = 3333
BUFFER_SIZE = 1024

class State:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def add(self, key, value):
        with self.lock:
            self.data[key] = value
        return f"{key} added"

    def get(self, key):
        with self.lock:
            return self.data.get(key, "Key not found")

    def remove(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                return f"{key} removed"
            return "Key not found"

    def list_items(self):
        with self.lock:
            return ",".join(f"{key}={value}" for key, value in self.data.items())

    def count(self):
        with self.lock:
            return len(self.data)

    def clear(self):
        with self.lock:
            self.data.clear()

    def update(self, key, value):
        with self.lock:
            if key not in self.data:
                return False
            self.data[key] = value
            return True

    def pop(self, key):
        with self.lock:
            if key not in self.data:
                return None
            return self.data.pop(key)

state = State()

def process_command(command):
    parts = command.split()
    if not parts:
        return "Invalid command format", False

    cmd = parts[0].lower()

    if cmd == "add":
        if len(parts) < 3:
            return "Invalid command format", False
        return state.add(parts[1], " ".join(parts[2:])), False

    if cmd == "get":
        if len(parts) != 2:
            return "Invalid command format", False
        return state.get(parts[1]), False

    if cmd == "remove":
        if len(parts) != 2:
            return "Invalid command format", False
        return state.remove(parts[1]), False

    if cmd == "list":
        if len(parts) != 1:
            return "ERROR invalid command", False
        return f"DATA|{state.list_items()}", False

    if cmd == "count":
        if len(parts) != 1:
            return "ERROR invalid command", False
        return f"DATA {state.count()}", False

    if cmd == "clear":
        if len(parts) != 1:
            return "ERROR invalid command", False
        state.clear()
        return "OK all data deleted", False

    if cmd == "update":
        if len(parts) < 3:
            return "ERROR invalid command", False
        if not state.update(parts[1], " ".join(parts[2:])):
            return "ERROR invalid key", False
        return "OK data updated", False

    if cmd == "pop":
        if len(parts) != 2:
            return "ERROR invalid command", False
        value = state.pop(parts[1])
        if value is None:
            return "ERROR invalid key", False
        return f"DATA {value}", False

    if cmd == "quit":
        if len(parts) != 1:
            return "ERROR invalid command", False
        return "OK bye", True

    return "ERROR unknown command", False

def handle_client(client_socket):
    with client_socket:
        while True:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    break

                command = data.decode('utf-8').strip()
                response, should_close = process_command(command)
                
                response_data = f"{len(response)} {response}".encode('utf-8')
                client_socket.sendall(response_data)

                if should_close:
                    break

            except Exception as e:
                error_message = f"ERROR {str(e)}"
                response_data = f"{len(error_message)} {error_message}".encode('utf-8')
                client_socket.sendall(response_data)
                break

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"[SERVER] Listening on {HOST}:{PORT}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"[SERVER] Connection from {addr}")
            threading.Thread(target=handle_client, args=(client_socket,)).start()

if __name__ == "__main__":
    start_server()
