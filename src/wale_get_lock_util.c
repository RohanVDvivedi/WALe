#include<wale_get_lock_util.h>

pthread_mutex_t* get_wale_lock(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		return &(wale_p->internal_lock);
	else
		return wale_p->external_lock;
}