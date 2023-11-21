import iosock
import signal
import threading
import time
import math

server = iosock.Server()

def unpacking(source_bytes: bytes, byteorder: str = 'little') -> bytes:
    bit8_length = 1
    length_of_length = int.from_bytes(source_bytes[:bit8_length], byteorder=byteorder)
    source_length = int.from_bytes(source_bytes[bit8_length:(bit8_length+length_of_length)], byteorder=byteorder)
    start_index = bit8_length+length_of_length
    end_index = bit8_length+length_of_length+source_length
    if len(source_bytes) == end_index:
        return source_bytes[start_index:end_index]
    else:
        return None

starter = b'%w$d#'
closer = b'&sa@f#d$'

# packed_recv_bytes_removed = recv_bytes.removeprefix(starter)
# packed_recv_bytes_removed = recv_bytes.removesuffix(closer)
# unpacked_recv_bytes = unpacking(packed_recv_bytes_removed)
# print(unpacked_recv_bytes)

def signal_handler(num_recv_signal, frame):
    print(f"\nGet Signal: {signal.Signals(num_recv_signal).name}")
    server.stop()


def recv_threading():
    
    
    recv_data = {}
    time_recv_data = {}
    while True:
        recv_temp_data = server.recv()
        if not recv_temp_data:
            break
        fileno = recv_temp_data['fileno']
        data = recv_temp_data['data']
        
        if fileno in recv_data:
            if recv_data[fileno] == b'':
                time_recv_data[fileno] = time.time()
            recv_data[fileno] += data
        else:
            recv_data[fileno] = data
            time_recv_data[fileno] = time.time()
        
        bit8_length = 1
        start_index = len(starter)
        end_index = len(starter)+bit8_length
        if end_index <= len(recv_data[fileno]) and recv_data[fileno][:len(starter)] == starter:
            length_of_length_bytes = recv_data[fileno][start_index:end_index]
            length_of_length = int.from_bytes(length_of_length_bytes, byteorder='little')
            
            start_index = end_index
            end_index = end_index + length_of_length
            # print(f"{recv_data[fileno][:len(starter)]} == {starter}? {start_index}:{end_index}")
            if end_index <= len(recv_data[fileno]):
                length_bytes = recv_data[fileno][start_index:end_index]
                source_length = int.from_bytes(length_bytes, byteorder='little')
                
                start_index = end_index
                end_index = end_index + source_length
                # print(f"source_length:{source_length} {start_index}:{end_index} {end_index} <= {len(recv_data[fileno])}({end_index <= len(recv_data[fileno])}) {recv_data[fileno][end_index:end_index+len(closer)]} == {closer}({recv_data[fileno][end_index:end_index+len(closer)] == closer})")
                if end_index + len(closer) <= len(recv_data[fileno]) and recv_data[fileno][end_index:end_index+len(closer)] == closer:
                    recv_bytes:bytes = recv_data[fileno][start_index:end_index]
                    recv_data[fileno] = recv_data[fileno][end_index+len(closer):]
                    end = time.time()
                    # server.send(fileno, recv_bytes)
                    # src = data.replace(b'abcdefghijklmnop', b'')
                    text_print = f'[{fileno}] recv {len(recv_bytes)} bytes. over:{len(recv_data[fileno])}'
                    # text_print += f"[{fileno}] {len(src)}"
                    # if src != b'':
                    #     text_print += f"\n{src}"
                    text_print += f' time elapsed: {end - time_recv_data[fileno]}'
                    print(text_print)
                    
                    
                    # if data:
                    #     text_print = ''
                    #     packed_recv_bytes_removed = data.removeprefix(starter)
                    #     packed_recv_bytes_removed = packed_recv_bytes_removed.removesuffix(closer)
                    #     unpacked_recv_bytes = unpacking(packed_recv_bytes_removed)
                    #     if unpacked_recv_bytes:
                    #         text_print += f"[{fileno}] {len(unpacked_recv_bytes)} {unpacked_recv_bytes[:10]}...{unpacked_recv_bytes[-10:]}"
                    #     else:
                    #         src = data.replace(b'abcdefghijklmnop', b'X')
                    #         text_print += f"[{fileno}] {len(src)}\n{src}"
                        
                    #     server.send(fileno, data)
                    #     end = time.time()
                    #     text_print += f' time elapsed: {end - time_recv_data[fileno]}'
                    #     print(text_print)
                    
                
            
    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start('218.55.118.203', 59012)
    print("Server Start.")
    
    recv_thread = threading.Thread(target=recv_threading)
    recv_thread.start()
    
    server.join()
    recv_thread.join()
    
