import argparse
import socket
import threading
import copy
import pickle

TCP_IP = "127.0.0.1"
nodes = {}

def main():
	#COMMAND LOOP
	nodes[0] = node(4000, 0)
	f = open("keys.txt")
	for line in f:
		line = line.split()
		nodes[0].keys[int(line[0])] = line[1]
	t = threading.Thread(target=start_node, args=(nodes[0],))
	t.setDaemon(True)
	t.start()
	running = 1
	while running == 1:
		command = raw_input("please enter new command:\n")
		command = command.split()
		if command[0] == 'exit':
			break
		elif command[0] == "join":
			p = int(command[1])
			nodes[p] = node(4000+p, p)
			t = threading.Thread(target=start_node, args=(nodes[p],))
			t.setDaemon(True)
			t.start()
			nodes[p].join(0)
		elif command[0] == 'find':
			p = int(command[1])
			if p in nodes:
				k = int(command[2])
				print nodes[p].find(k)
		elif command[0] == 'leave':
			p = int(command[1])
			for key in nodes[p].keys:
				send_recv("transfer_key_post "+str(key)+" "+str(nodes[p].keys[key]), nodes[p].fingertable[1].node)
			send_recv("update_predecessor "+str(nodes[p].predecessor_id), nodes[p].fingertable[1].node)
			send_recv("update_successor "+str(nodes[p].fingertable[1].node), nodes[p].predecessor_id)
			send_recv("update_others", nodes[p].fingertable[1].node)
			nodes[p].running = False
			send_recv("close", p)
			del nodes[p]
		elif command[0] == 'show':
			if command[1] == 'all':
				for key in nodes:
					out = str(key)
					temp_keys = []
					for index in nodes[key].keys:
						temp_keys.append(int(index))
					temp_keys = sorted(temp_keys)
					for entry in temp_keys:
						out += " "+str(entry)
					print out
			else:
				out = command[1]
				temp_keys = []
				for index in nodes[int(command[1])].keys:
					temp_keys.append(int(index))
				temp_keys = sorted(temp_keys)
				for key in temp_keys:
					out += " "+str(key)
				print out
		else:
			pass

class finger_entry:
	def __init__(self, start, end, node):
		self.start = start
		self.end = end
		self.node = node

class node:
	def __init__(self, port, idno):
		self.running = True
		self.port = port
		self.idno = idno
		self.predecessor_id = 0
		self.keys = {}
		self.fingertable = {}
		for i in range(1, 8+1):
			self.fingertable[i] = finger_entry((idno+2**(i-1))%256, idno+2**(i)%256, self.idno)

	#returns successor of idno
	def find_successor(self, idno):
		return self.find_predecessor(idno)[1]

	#returns tuple of the predecessor of idno and the predecessors successor
	def find_predecessor(self, idno):
		temp = (self.idno, self.fingertable[1].node)
		while not is_between(idno, temp[0]+1, temp[1]+1):
			x = send_recv("find_cpf "+str(idno), temp[0]).split(" ")
			temp = (int(x[0]), int(x[1]))
		return temp


	#finds the finger of the caller that is the closest finger that precedes idno
	def find_cpf(self, idno):
		for i in range(8, 0, -1):
			if is_between(self.fingertable[i].node, self.idno+1, idno):
				return (self.fingertable[i].node, int(send_recv("get_successor", self.fingertable[i].node)))
		return (self.idno, self.fingertable[i].node)

	def join(self, node_id):
		if node_id in nodes:
			self.init_finger_table(node_id)
			self.update_others()
		else:
			for i in range(1, 8+1):
				self.fingertable[i].node = self.idno
			self.predecessor_id = self.idno
		for i in range(self.predecessor_id+1, self.idno+1):
			x = send_recv("transfer_key_get "+str(i), self.fingertable[1].node).split()
			self.keys[int(x[0])] = x[1]

	def init_finger_table(self, node_id):
		self.fingertable[1].node = int(send_recv("find_successor "+str(self.fingertable[1].start), node_id))
		self.predecessor_id = int(send_recv("get_predecessor", self.fingertable[1].node))
		send_recv("update_predecessor "+str(self.idno), self.fingertable[1].node)
		for i in range(1, 8+1-1):
			if is_between(self.fingertable[i+1].start, self.idno, self.fingertable[i].node):
				self.fingertable[i+1].node = self.fingertable[i].node
			else:
				self.fingertable[i+1].node = int(send_recv("find_successor "+str(self.fingertable[i+1].start)))

	def update_others(self):
		for i in range(1, 8+1):
			p = self.find_predecessor((self.idno-2**(i-1))%256)[0]
			send_recv("update_finger_table "+str(self.idno)+" "+str(i), p)

	def update_finger_table(self, s, i):
		#the paper is wrong, the second argument to this is different than the paper
		if is_between(s, self.fingertable[i].start, self.fingertable[i].node):
			self.fingertable[i].node = s
			p = self.predecessor_id
			send_recv("update_finger_table "+str(s)+" "+str(i), p)

	def find(self, key):
		if key not in self.keys:
			if self.find_cpf(key)[0] == self.idno:
				return send_recv("find_key "+str(key), self.fingertable[1].node)
			else:
				return send_recv("find_key "+str(key), self.find_cpf(key)[0])
		else:
			return self.idno

def start_node(node):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, node.port))
	s.listen(100)
	while node.running:
		(conn, addr) = s.accept()
		t = threading.Thread(target=process_request, args=(node,conn))
		t.setDaemon(True)
		t.start()
		
def process_request(node, conn):
	req = ""
	while True:
		data = conn.recv(1024);
		if data[-1] == "\n": 
			req += data
			break
		else:
			req += data			
	req = req[:-1]
	if req == "":
		return
	req = req.split(" ")
	if req[0] == "find_successor":
		conn.send(str(node.find_successor(int(req[1])))+"\n")
	if req[0] == "find_predecessor":
		x = node.find_predecessor(int(req[1]))
		conn.send(str(x[0])+" "+str(x[1])+"\n")
	if req[0] == "find_cpf":
		x = node.find_cpf(int(req[1]))
		conn.send(str(x[0])+" "+str(x[1])+"\n")
	if req[0] == "update_others":
		node.update_others()
		conn.send("ack\n")
	if req[0] == "update_finger_table":
		node.update_finger_table(int(req[1]), int(req[2]))
		conn.send("ack\n")
	if req[0] == "update_predecessor":
		node.predecessor_id = int(req[1])
		conn.send("ack\n")
	if req[0] == "update_successor":
		node.fingertable[1].node = int(req[1])
		conn.send("ack\n")
	if req[0] == "get_predecessor":
		conn.send(str(node.predecessor_id)+"\n")
	if req[0] == "get_successor":
		conn.send(str(node.fingertable[1].node)+"\n")
	if req[0] == "close":
		conn.send("ack\n")
	if req[0] == "transfer_key_get":
		conn.send(req[1]+" "+node.keys[int(req[1])]+"\n")
		del node.keys[int(req[1])]
	if req[0] == "transfer_key_post":
		node.keys[int(req[1])] = req[2]
		conn.send("ack\n")
	if req[0] == "find_key":
		conn.send(str(node.find(int(req[1])))+"\n")
	return

def send_recv(command, idno):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, 4000+idno))
	s.send(command+"\n")
	response = ""
	while True:
		data = s.recv(1024)
		if data[-1] == "\n": 
			response += data
			break
		response += data
	s.close()

	return response[:-1]

def is_between(x, a, b):
	a = a %256
	b = b %256
	if b <= a:
		if x >= a and x < 256:
			return True
		if x >= 0 and x < b:
			return True
	else:
		if x >= a and x < b:
			return True
	return False


if __name__ == '__main__':
	main()
