#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#define VTOC_SHIFT	1<<11
#define MBOX_MAGIC	0xCBFFCBFF
#define MBOX_VERSION	0x1
#define MBOX_OWNER_MAX  256
#define MBOX_RETRY_MAX  2
#define MBOX_LOCK_OFFSET VTOC_SHIFT

#define MBOX_LOCKED(data)	(!data->mbox_wr_lock) 	
#define MBOX_DATA_SIZE	sizeof(ha_mbox_t)
#define IMPORT_SCRIPT "python /project/import.py pool1 de2 de1 %s"

static char dev_path[256];



#define USAGE \
	"Usage: %s [-i] <device path> <hostname> \n"\
	"	-i init mbox partition\n"


enum {
	MBOX_SUCCESS = 0,
	MBOX_READ_ERROR,
	MBOX_WRITE_ERROR,
	MBOX_LOST_WRITE,
	MBOX_NO_MBOX_DATA
};

enum {
	MBOX_OWNER_INITIALIZING = 0,
	MBOX_PARTNER_TAKING_OVER,
	MBOX_UPDATE_HB
};


struct cloudbyte_ha_mbox
{
//	uint32_t	mbox_padding[VTOC_SHIFT]; /* unused for VTOC compatability */
	uint32_t	mbox_magic;		  /* magic to recognize cb mbox data */
	uint32_t	mbox_version;		  /* version of mbox */
	uint32_t	mbox_wr_lock;		  /* write lock for update */
	uint64_t	mbox_state;			
	char		mbox_act_owner[MBOX_OWNER_MAX];   /* actual owner of the mbox disk */
	char		mbox_cur_owner[MBOX_OWNER_MAX];  /* current owner, current_owner != actual_owner if partner takenover */
	uint64_t	mbox_heart_beat; 	  /* heart_beat count ticks since uptime */
	
};

typedef struct cloudbyte_ha_mbox ha_mbox_t;

static int
mbox_not_owner(ha_mbox_t *data, char *hostname)
{
	return strcmp(data->mbox_cur_owner, hostname);
}

static int
mbox_in_takeover(ha_mbox_t *data, char *hostname)
{
	return strcmp(data->mbox_cur_owner, data->mbox_act_owner);
}

static int
mbox_write_lock_data(int fd, ha_mbox_t *data)
{
	ssize_t  size = 0;
	
	if ( time(NULL) - data->mbox_heart_beat >= DEBUG_THRESHOLD ) {
		printf("Last write greater than threshold\n");
		return MBOX_LOST_WRITE;
	}
	size = pwrite(fd, (char*)data, 512, (MBOX_LOCK_OFFSET));
	if (size <= 0) {
		printf("failed to write to disk");
		return MBOX_WRITE_ERROR;
	}
	fsync(fd);
	return MBOX_SUCCESS;
}


static inline int
mbox_read_lock_data(int fd, ha_mbox_t *data)
{
	ssize_t	size = 0;				
	int 	status = 0;
	size = pread(fd, (char*)data, 512, (MBOX_LOCK_OFFSET));
	if (size < 0) {				
		perror("");
		status = MBOX_READ_ERROR;
	}
	return status;
}

	

static inline int
mbox_read_data(int fd, ha_mbox_t *data)
{
	ssize_t	size = 0;				
	ssize_t tsize = 0;
	void 	*raw_data;
	int	status = MBOX_SUCCESS;
	raw_data = malloc(MBOX_DATA_SIZE + 512);
	memset(raw_data, 0, MBOX_DATA_SIZE + 512);
	while (size <= MBOX_DATA_SIZE) {
		tsize = pread(fd, (char*)raw_data+size, 512, size + MBOX_LOCK_OFFSET);
		if (tsize < 0) {				
			perror("");
			status = MBOX_READ_ERROR;
			goto out;
		}					
		size +=tsize;
	}
	memcpy(data, raw_data, MBOX_DATA_SIZE);
	if (data->mbox_magic != MBOX_MAGIC &&	
	    data->mbox_version != MBOX_VERSION) {
		status = MBOX_NO_MBOX_DATA;			
		goto out;
	}
out:
	free(raw_data);
	return status;
}

static inline int
mbox_write_data(int fd, ha_mbox_t *data)
{
	ssize_t  size = 0;
	ssize_t  tsize;

	if ( time(NULL) - data->mbox_heart_beat >= 10 ) {
		printf("Last write greater than threshold\n");
		return -1;
	}
	while (size <= MBOX_DATA_SIZE) {
		tsize = pwrite(fd, ((char*)data+size), 512, size + MBOX_LOCK_OFFSET);
		if (tsize <= 0) {
			printf("failed to write to disk");
			return -1;
		}
		size += tsize;
	}
	fsync(fd);
	return MBOX_SUCCESS;
}

static int
mbox_get_write_lock(int fd, ha_mbox_t *data)
{
	ha_mbox_t	cur_data, tmp_data;
	int		status; 
	uint32_t	signature;
	int		retry = MBOX_RETRY_MAX;

	signature = random() + time(NULL);
	data->mbox_wr_lock = signature;
	if ( mbox_write_lock_data(fd, data) == MBOX_WRITE_ERROR ) {
			return -1;
	}
	if (mbox_read_lock_data(fd, &tmp_data)) {
		printf("Failed to read data after locking\n");
		return -1;
	}
	if (tmp_data.mbox_wr_lock == signature) {
		return 0;
	}
	if (tmp_data.mbox_wr_lock) {
		do {
			sleep(1);		
			if (mbox_read_lock_data(fd, &tmp_data)) {
				printf("Failed to read data after locking\n");
				return -1;
			}
			if (!MBOX_LOCKED((&tmp_data))) {
				return -1;
			}
		} while(MBOX_LOCKED((&tmp_data)) && (--retry));
	} else {
		return -1;
	}
	/* Retries elapsed hence force aquire lock */
	printf("Other node did not release lock. force write\n");
	return mbox_write_lock_data(fd, data);
	
}

	
		
	
		
static int
mbox_init_disk(char *dev_path, char* hostname) 
{
	int status;
	int fd;
	ha_mbox_t	cur_data;
	ha_mbox_t	*data = NULL;
	
	if ((fd = open(dev_path, O_RDWR|O_DIRECT|O_SYNC)) < 0) {
		perror("");
		return -1;
	
	}
	/*if (mbox_read_data(fd, &cur_data) == 0) {
		if (mbox_not_owner(&cur_data, hostname) && 
		    !mbox_in_takeover(&cur_data, hostname)) {
			printf("disk already initialized\n");
			status  = 1;
			goto out;
		}
	}
	*/
	memset(&cur_data, 0, sizeof(cur_data));
	cur_data.mbox_magic = MBOX_MAGIC;
	cur_data.mbox_version = MBOX_VERSION;
	strncpy(cur_data.mbox_act_owner, 
			hostname, MBOX_OWNER_MAX);
	strncpy(cur_data.mbox_cur_owner, 
			hostname, MBOX_OWNER_MAX);
	cur_data.mbox_state = MBOX_OWNER_INITIALIZING;
	cur_data.mbox_heart_beat = time(NULL);
	status = mbox_get_write_lock(fd, &cur_data);
	if (status < 0) {
		printf("could not get lock on disk\n");
		goto out;
	}
	cur_data.mbox_heart_beat = time(NULL);
	cur_data.mbox_wr_lock = 0;
	status = mbox_write_data(fd, &cur_data);
out:
	close(fd);
	return status; 
}

static int 
mbox_takeover(int fd, ha_mbox_t *cur_data, char *hostname)
{

	ha_mbox_t new_data;
	int	  retry = MBOX_RETRY_MAX;
	ssize_t	  size;
	char	  *cmd;

	memcpy(&new_data, cur_data, sizeof(new_data));	
	strcpy(new_data.mbox_cur_owner, hostname);
	new_data.mbox_wr_lock = 0;
	new_data.mbox_state = MBOX_PARTNER_TAKING_OVER;
	new_data.mbox_heart_beat = time(NULL);
	
	do {	
		if (mbox_get_write_lock(fd, &new_data) == 0) {
			break;
		}
		sleep(1);
		if (!mbox_read_data(fd, cur_data)) {
			return -1;
		} 
		if (cur_data->mbox_heart_beat > new_data.mbox_heart_beat) {
			return 0;	
		}
		retry--;
	} while (retry);
	
	if (retry <= 0) return -1;
	cmd = malloc(64 + strlen(dev_path));
	sprintf(cmd, IMPORT_SCRIPT, dev_path);
	system(cmd);
	free(cmd);
	new_data.mbox_heart_beat = time(NULL);
	size = mbox_write_data(fd, &new_data);
	if (size) {
		printf("Write failed on mbox disk\n");
		return -1;
	}
	return 0;
}

	
	
static int
mbox_update_hb(int fd, ha_mbox_t* hb_data, char* hostname)
{
	int retry = MBOX_RETRY_MAX;
	ha_mbox_t	cur_data;	
#ifdef DEBUG
	static 	int retries = 0;
	if (retries < 5) {
		retries++; 	
	} else {
		printf("sleeping for sometime\n");
		sleep(DEBUG_SLEEP);
		retries = 0;
	}
#endif
	
	hb_data->mbox_state = MBOX_UPDATE_HB;
	if (mbox_get_write_lock(fd, hb_data) == 0) {
		hb_data->mbox_heart_beat = time(NULL);
		return mbox_write_data(fd, hb_data);
	}

	mbox_read_data(fd, &cur_data);
	if (strcmp(cur_data.mbox_cur_owner, hostname) != 0) {
		system("reboot");
	}
	return 0;
}
		

static int
mbox_start_comm(char *dev, char *hostname)
{
	int fd;
	int takeover_retry = MBOX_RETRY_MAX;
	ha_mbox_t	*prev_data = NULL;
	ha_mbox_t	cur_data;
	int		status;
	
	fd = open(dev, O_RDWR|O_DIRECT|O_SYNC);
	if (fd < 0) {
		perror("");
		return -1;
	}

	status = mbox_read_data(fd, &cur_data);
	do {
		if(!takeover_retry) {
			/* Node heart beat failed, lets do takeover */
			printf("partner went down. Taking over\n");
			if (mbox_takeover(fd, &cur_data, hostname) != 0) {
				return -1;
			}
			takeover_retry = MBOX_RETRY_MAX;
			sleep(2);
			continue;
		}
		status = mbox_read_data(fd, &cur_data);
		if (status) {
			/* You might enter here when partner is looking for data
                         * even before the disk initialization by owner.
			 */
			sleep(2);
			continue;
		}
		if (strcmp(cur_data.mbox_cur_owner, hostname) == 0) {
			/* IM the owner udpate heart beat */
			if (strcmp(cur_data.mbox_cur_owner, cur_data.mbox_act_owner) == 0){
				printf("Owner Updating heartbeat \n");
			} else {
				printf("Partner Updating heartbeat \n");
			}
			cur_data.mbox_heart_beat = time(NULL);	
			mbox_update_hb(fd, &cur_data, hostname);
			sleep(2);
		} else {
			/* I'm the partner, check the health */
			if (strcmp(cur_data.mbox_act_owner, hostname) == 0){
				printf("Partner took over, panic\n");
				system("reboot");
			}
			printf("Partner checking heartbeat of owner\n");
			if (prev_data == NULL) {
				prev_data = (ha_mbox_t*)malloc(sizeof(cur_data));
				memcpy(prev_data, &cur_data, sizeof(cur_data));
			} 
			else if (prev_data->mbox_heart_beat == cur_data.mbox_heart_beat) {
				takeover_retry--;
			} else { 
				takeover_retry = MBOX_RETRY_MAX;
				memcpy(prev_data, &cur_data, sizeof(cur_data));
			}
			sleep(3);
		}
	} while(1);
				
}
				
		
			
					
	


int main(int argc, char *argv[])
{
	
	char 	hostname[MBOX_OWNER_MAX + 1];	

	if (argc < 3) {
		printf(USAGE, argv[0]);
		return -1;
	}

	if (strcmp(argv[1], "-i") == 0) {
		if (argc == 3) {
			printf(USAGE, argv[0]);
			return -1;
		}
		
		strncpy(dev_path, argv[2], sizeof(dev_path));
		if (mbox_init_disk(dev_path, argv[3]) != 0) {
			printf("Failed to initialize mbox disk\n");
			return -1;
		}
		return mbox_start_comm(argv[2], argv[3]);
	} else {
		strncpy(dev_path, argv[1], sizeof(dev_path));
		return mbox_start_comm(dev_path, argv[2]);
	}
}

	

	
	
		
	

	
	
