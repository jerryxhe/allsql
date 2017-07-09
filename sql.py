import operator

def _dict2sql(kw):
	return "".join(reduce(operator.__add__, [[str(k), "=", repr(unicode(v))[1:],","] 
                                        for k,v in kw.iteritems()])[:-1])
