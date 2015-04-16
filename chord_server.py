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
			print "yo"
		elif command[0] == 'find':
			print "not yet implemented\n"
		elif command[0] == 'leave':
			print "not yet implemented\n"
		elif command[0] == 'show':
			print "not yet implemented\n"
			if command[1] == 'all':
				for key in nodes:
					print "NODE "+str(nodes[key].idno)
					print "SUCCESSOR = "+str(nodes[key].successor_id)
					print "PREDECESSOR = "+str(nodes[key].predecessor_id)
					print "FINGER TABLE:"
					for i in range(1, 8+1):
						print "RANGE: "+"("+str(nodes[key].fingertable[i].start)+","+str(nodes[key].fingertable[i].end)+")"
						print "SUCCESSOR: "+str(nodes[key].fingertable[i].successor_id)
			else:
				pass
		else:
			pass

class finger_entry:
	def __init__(self, start, end, successor):
		self.start = start
		self.end = end
		self.successor_id = successor

class node:
	def __init__(self, port, idno):
		self.port = port
		self.idno = idno
		self.predecessor_id = 0
		self.successor_id = 0
		self.keys = {}
		self.fingertable = {}
		for i in range(1, 8+1):
			self.fingertable[i] = finger_entry((idno+2**(i-1))%256, idno+2**(i)%256, self.idno)


	def find_successor(self, idno):
		return self.find_predecessor(idno)[1]

	def find_predecessor(self, idno):
		print self.idno
		print idno
		temp = (self.idno, self.successor_id)
		while not is_between(temp[0]+1, temp[1]+1, idno):

			if temp[0] == self.idno:
				temp = self.find_cpf(idno)
			else:
				x = send_recv("find_cpf "+str(idno), temp[0]).split(" ")
				temp = (x[0], x[1])

		return temp

	def find_cpf(self, idno):
		for i in range(8, 0, -1):
			if is_between(self.idno+1, idno, self.fingertable[i].successor_id):
				return (self.fingertable[i].successor_id, int(send_recv("get_successor", self.fingertable[i].successor_id)))
		return (self.idno, self.successor_id)

	def join(self, node_id):
		if node_id in nodes:
			self.init_finger_table(node_id)
			self.update_others()
		else:
			for i in range(1, 8+1):
				self.fingertable[i].successor_id = self.idno
			self.predecessor_id = self.idno

	def init_finger_table(self, node_id):
		self.fingertable[1].successor_id = int(send_recv("find_successor "+str(self.fingertable[1].start), node_id))
		self.successor_id = self.fingertable[1].successor_id
		print self.successor_id
		self.predecessor_id = int(send_recv("get_predecessor", self.successor_id))
		send_recv("update_predecessor "+str(self.idno), self.successor_id)
		for i in range(1, 8+1-1):
			if is_between(self.idno, self.fingertable[i].successor_id, self.fingertable[i+1].start):
				self.fingertable[i+1].successor_id = self.fingertable[i].successor_id
			else:
				self.fingertable[i+1].successor_id = int(send_recv("find_successor "+str(self.fingertable[i+1].start), node_id))

	def update_others(self):
		for i in range(1, 8+1):
			p = self.find_predecessor((self.idno-2**(i-1))%256)[0]
			send_recv("update_finger_table "+str(self.idno)+" "+str(i), p)

	def update_finger_table(self, node_id, index):
		if is_between(self.idno, self.fingertable[index].successor_id, node_id):
			self.fingertable[index].successor_id = node_id
			p = self.predecessor_id
			send_recv("update_finger_table "+str(node_id)+" "+str(index), p)

def start_node(node):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, node.port))
	s.listen(9)
	while True:
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
		conn.send(str(node.find_predecessor(int(req[1]))[0])+" "+str(node.find_predecessor(int(req[1]))[1])+"\n")
	if req[0] == "find_cpf":
		conn.send(str(node.find_cpf(int(req[1]))[0])+" "+str(node.find_cpf(int(req[1]))[1])+"\n")
	if req[0] == "update_finger_table":
		node.update_finger_table(int(req[1]), int(req[2]))
		conn.send("ack\n")
	if req[0] == "update_predecessor":
		node.predecessor_id = int(req[1])
		conn.send("ack\n")
	if req[0] == "get_predecessor":
		conn.send(str(node.predecessor_id)+"\n")
	if req[0] == "get_successor":
		conn.send(str(node.successor_id)+"\n")
	return

def send_recv(command, idno):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, 4000+idno))
	print command
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

def is_between(a, b, x):
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
