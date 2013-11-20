
import sys
infile=sys.argv[1]
outfile=sys.argv[2]
print infile
print outfile
#f=open("/home/vadiraj/Project/msg_read.log", "r")
#fo=open("/home/vadiraj/Project/msgOut3", "w")
fin = open(infile, "r")
fout = open(outfile, "w")
i=0;
for line in fin:
	i = i + 1
	if (i%2 != 0):
		fout.write(line)

fin.close()
fout.close()
fread=open(outfile, "r")
data_list = [["x","x","x","x"]]
time_list = [["x", 0,0,0,0]]

for line in fread:
	line.rstrip()
	sp = line.split(' ')
	if (len(sp) < 15): 
		continue
	ln = sp[6]
	if (len(ln.split(":")) != 2):
		continue
	time = ln.split(':')[0]
	dx = ln.split(':')[1]
	if ( dx == "g_down" ):
		bp = sp[9]
		provider = sp[11]
		data_len = sp[14].rstrip()
		cmd 	 = sp[len(sp) - 1].rstrip()
		for ele in data_list:
			if (ele[0] == bp and ele[2] == provider and ele[3] == data_len and ele[4] == cmd):
				time_ele = ele[5]
				time_ele[1] = time
				break;
	elif (dx == "g_io_deliver" ):
		bp = sp[8]
		provider = sp[12]
		data_len = sp[14].rstrip()
		cmd 	 = sp[len(sp) - 1].rstrip()
		for ele in data_list:
			if (ele[0] == bp and ele[2] == provider and ele[3] == data_len and ele[4] == cmd):
				time_list = ele[5]
				time_list[2] = time
				break;
	elif (dx == "g_up"):
		bp 	 = sp[9]
		provider = sp[13]
		data_len = sp[len(sp) - 3].rstrip()
		cmd	 = sp[len(sp) - 1].rstrip()
		for ele in data_list:
			if (ele[0] == bp and ele[2] == provider and ele[3] == data_len and ele[4] == cmd):
				time_list = ele[5]
				time_list[3] = time
				gtime_diff = (float(time_list[1]) - float(time_list[0]))/1000000
				dtime_diff = (float(time_list[2]) - float(time_list[1]))/1000000 
				guptime_diff = (float(time) - float(time_list[2]))/1000000
				print "| {0:17s} | {1:5s} |{2:36s} | {3:36s} | {4:2s}"\
				      "| {5}ms | {6}ms | {7}ms"\
				      " |".format(ele[0], ele[3], ele[1], ele[2], ele[4],
						  gtime_diff, dtime_diff, guptime_diff)
				#print ele
				data_list.remove(ele)
				break;

	else:
		if (dx.find("g_io_request") < 0):
			continue		
		bp = dx.split('(')[1][:-1]
		if (sp[10].find("(") < 0 or sp[8].find("(") < 0):
			continue
		initiator  = sp[8].split('(')[1][:-1]
		provider   = sp[10].split('(')[1][:-1]
		data_len   = sp[12].rstrip()
		cmd	   = sp[14].rstrip()
		time_list  = [time,0,0,0]
		if (len(data_list) == 0):
			data_list = [[bp, initiator, provider, data_len, cmd, time_list]]
		else:
			for ele in data_list:
				if (ele[0] == bp):
					data_list.remove(ele)
			data_list.append([bp, initiator, provider, data_len, cmd, time_list])
fread.close()






