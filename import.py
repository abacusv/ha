

import os
import subprocess
import sys

def run_process(cmd):
	print "Executing cmd: {0}".format(cmd)
	proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
	(out, err) = proc.communicate()
	return (out, err, proc.returncode)


def get_multipath_list(import_out, pool_name):
	i = -1
	j = -1
	multipaths = 0
	multipath_label = list()
	for line in import_out.splitlines():
		if j == 0:
			if "multipath" in line:
				multipath_label.append(line.split("/")[1].split(" ")[0].strip())
				multipaths = multipaths + 1
				j = 1
			elif "Pool:" in line:
				j = -1
			else:
				j = 1
		if i == 0:
			if pool_name in line:
				j = 1
			i = -1
		if "config:" in line:
			i = 2
		i = i - 1
		j = j - 1
	return multipath_label

def get_disk_name(multipath_out):
	disk = None
	for line in multipath_out.splitlines():
		if "Name:" in line and "multipath" not in line:
			disk = line.split(" ")[2].strip()
			break
	return disk


def get_disks(multipath_label):
	dev_list = list()
	for label in multipath_label:
		(lout, err, ret) = run_process("gmultipath list {0}".format(label))
		if ret != 0:
			return dev_list
		disk = get_disk_name(lout)
		if disk != None:
			dev_list.append(disk)
	return dev_list
	


def get_disk_ids(pool_name):
	(out, err, ret) = run_process("zpool import")
	if ret != 0:
		return list()
	multipaths = get_multipath_list(out, pool_name)
	if len(multipaths) <= 0:
		print "No pool with {0} found\n".format(pool_name)
	return get_disks(multipaths)

def reserve_disks(dev_list, rkey, srkey):
	ret = 1
	for disk in dev_list:
		cmd = "sg_persist --out --register --param-sark={0} /dev/{1}".format(srkey, disk)
		(out, err, ret) = run_process(cmd)
		cmd = "sg_persist --out --preempt-abort --param-sark={0} --param-rk={1} --prout-type=1 /dev/{2}".format(srkey, rkey, disk)
		(out, err, ret) = run_process(cmd)
		cmd = "sg_persist --out --reserve --param-rk={0} --prout-type=1 /dev/{1}".format(srkey, disk)
		(out, err, ret) = run_process(cmd)
		if ret != 0:
			print "Failed to reserve the disk {0}:\n {1}".format(disk, err)
	return ret

def unreserve_disks(dev_list, srkey):
	ret = 1
	for disk in dev_list:
		cmd = "sg_persist --out --register --param-sark={0} /dev/{1}".format(srkey,disk)
		(out, err, ret) = run_process(cmd)
		cmd = "sg_persist -C --out --param-rk={0} /dev/{1}".format(srkey, disk)
		(out, err, ret) = run_process(cmd)
		if ret != 0:
			print "Failed to unreserve the disk {0}: {1}".format(disk, err)
	return ret


def import_pool(pool_name):
	cmd = "zpool import -f {0}".format(pool_name)
	(out, err, ret) = run_process(cmd)
	if ret != 0:
		print "Failed to import pool: {0}".format(err)
	else: 
		print "Pool imported successfully\n"
	return ret

def take_over_pool(pool_name, rkey, srkey):
	disk_ids = get_disk_ids(pool_name)
	status = reserve_disks(disk_ids, rkey, srkey)
	if status == 0:
		import_pool(pool_name)
	status = unreserve_disks(disk_ids, srkey)

def takeover_mbox(mbox_disk, rkey, srkey):
	dev_list = list()
	if "multipath" in mbox_disk:
		label = [mbox_disk.split("/")[-1]]
		dev_list = get_disks(label)
	else:
		dev_list.append(mbox_disk.split("/")[-1])

	ret = reserve_disks(dev_list, rkey, srkey)
	if ret != 0:
		unreserve_disks(dev_list, srkey)
	return dev_list
		
	
	
		
		

if __name__ == "__main__":
	if len(sys.argv) < 3:
		print "Usage:{0} <pool_name> <source_key> <remote_key>\n".format(sys.argv[0])
	else:
		pool_name = sys.argv[1]
		srkey = sys.argv[2]
		rkey = sys.argv[3]
		if len(sys.argv) > 3:
			mbox_disk = sys.argv[4]
			dev = takeover_mbox(mbox_disk.rstrip(), rkey, srkey)
		take_over_pool(pool_name, rkey, srkey)
		unreserve_disks(dev, srkey)

	

	
	
	
			
	

		
		
		
		
		
	

