#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"


PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for the library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;
}
