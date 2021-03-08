import urllib.request as urllib
import logging
import argparse
import csv


class Server:
    def __init__(self, host_name=None):
        self.current_req = None
        self.time_remaining = 0
        self.host_name = host_name

    def tick(self):
        if self.current_req != None:
            self.time_remaining = self.time_remaining - 1

        if self.time_remaining <= 0:
            self.current_req = None

    def busy(self):
        if self.current_req != None:
            return True

        else:
            return False

    def start_next(self, new):
        self.current_req = new
        self.time_remaining = new.get_request()


class Request:
    def __init__(self, time_stamp, seconds):
        self.seconds = seconds
        self.time_stamp = time_stamp
        

    def get_request(self):
        return self.seconds

    def wait_time(self, now):
        return now - self.time_stamp


class Queue:
    def __init__(self):
        self.arr = []

    def is_valid(self):
        return self.arr == []

    def add_queue(self, item):
        #self.arr.append(item)
        self.arr.insert(0, item)

    def remove_queue(self):
        return self.arr.pop()



def csv_api(url):
    csvAPI = urllib.urlopen(url).read()
    csv_rows = csvAPI.decode('utf-8').splitlines()
    res = csv.reader(csv_rows)
    arr = []
    for i in res:
        arr.append(i)
    return arr


def csv_values(ls):
    
    ls[0] = int(ls[0])
    ls[2] = int(ls[2])
    
    return [ls[0], ls[2]]



def queue_server(ls):
    queue = Queue()

    for i in ls:
        queue.add_queue(i)

    return queue


def print_many_servers_result(servers_dict):
    server_keys = list(servers_dict.keys())
    server_count = len(server_keys)
    sum_length_list = [len(servers_dict[key]) for key in server_keys]
    sum_result_list = [sum(servers_dict[key]) for key in server_keys]

    avg = sum(sum_result_list) / sum(sum_length_list)

    # for i, key in enumerate(server_keys): 
    print(
        f'Avg wait time is {avg} for {sum(sum_length_list)} list. Total servers  {server_count}')

def check_server(num):
    arr = []
    for i in range(num):
        arr.append(Server(host_name=i+1))
    return arr

def simulateManyServers(ls, n):
    q_req = Queue()
    servers = check_server(n)
    obj = {}
    time = 0
    server_queue = queue_server(servers)
    
    for i in ls:
        i = Request(csv_values(i)[0], csv_values(i)[1])
        q_req.add_queue(i)

    while not q_req.is_valid():
        server_node = server_queue.remove_queue()

        while server_node.busy():
            server_queue.add_queue(server_node)
            server_node.tick()
            time += 1
            server_node = server_queue.remove_queue()

        if not server_node.busy():
            next_request = q_req.remove_queue()
            host_name = server_node.host_name
            server_node.start_next(next_request)

            if not host_name in obj:
                obj[host_name] = []

            obj[host_name].append(next_request.wait_time(time))
            server_queue.add_queue(server_node)
        else:
            server_queue.add_queue(server_node)

    print_many_servers_result(obj)


def simulateOneServer(ls):
    server_req = Server()
    arr = []
    time = 0
    q_req = Queue()
    for i in ls:
        i = Request(csv_values(i)[0], csv_values(i)[1])
        q_req.add_queue(i)

    while not q_req.is_valid():

        while server_req.busy():
            server_req.tick()
            time += 1

        if not server_req.busy():
            next_request = q_req.remove_queue()
            arr.append(next_request.wait_time(time))
            server_req.start_next(next_request)

    wait = sum(arr) / len(arr)

    print(f'The average wait time is {wait} for a ls of {len(ls)} size')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--url')
    parser.add_argument('--servers', default=1)
    args = parser.parse_args()

    logging.basicConfig(filename='queue_errors.log',
                        level=logging.ERROR)

    logging.getLogger('errors')

    if args.url:
        try:
            res = csv_api(args.url)
            server_num = args.servers
            simulateOneServer(res) if server_num == 1 else simulateManyServers(
                res, int(server_num))

        except (ValueError, urllib.HTTPError):
            logging.error(f'Error: Something went wrong with {args.url}')


if __name__ == '__main__':
    main()
