from abc import ABCMeta,abstractproperty,abstractmethod
import operator

def _dict2sql(kw,joiner=",",escape_fun=lambda o:repr(o)[1:]):
	return "".join(reduce(operator.__add__, [[str(k), "=", escape_fun(unicode(v)),joiner] 
					for k,v in kw.iteritems()])[:-1])																			for k,v in kw.iteritems()])[:-1])

class SQLBaseClient:
	"""Abstract class to fit multiple SQL backends (e.g. MySQL, MSSQL, PostgreSQL)"""	
	__metaclass__= ABCMeta
	aliases={'password':'passwd','database':'db'}
	def __init__(self, **kw):
		for _real,_alias in self.aliases.iteritems():
			if _real not in kw and _alias in kw:
				kw[_real]=kw.pop(_alias)
		self._kw = kw
	@abstractmethod
	def cursor(self):
		if not hasattr(self, '_cursor'):
			self._cursor = self.conn.cursor()
		return self._cursor
	
	def rows(self):
		return [colname[0].decode("utf-8") for colname in self.cursor().description]
	
	def cleanup(self):
		try:
			self.cursor().close()
			self.conn.close()
		except:
			pass

	def retry_execute(self, query_str, debug_print=False, number_of_retries=8):
		if debug_print or (hasattr(self, 'DEBUG') and self.DEBUG):
			print(query_str)
		for i in xrange(number_of_retries):
			try:
				self.cursor().execute(query_str)
				return
			except Exception as e:
				print e
				if i%3 ==0: #maybe reconnecting to the DB will help
					self.cleanup()
					self.__init__(**self._kw)
	
	def ev(self, query_str):
		self.retry_execute(query_str)
		_rows = self.rows()
		for r in self.cursor():
			yield dict(zip(_rows, r))
			
	def select(self, tablename, **kw):
		sql_escape = self.conn.escape if hasattr(self.conn, 'escape') else lambda st:repr(unicode(st))[1:]
		query_str = "SELECT * FROM {} WHERE {}".format(tablename,_dict2sql(kw," AND ", sql_escape))
		return list(self.ev(query_str))

	def execute(self, query_str):
		self.retry_execute(query_str)
		self.conn.commit() # commits transaction to the SQL connection

	def update(self, _id, tablename, **kw):
		if not hasattr(self, '_id_field_name'):
			self._id_field_name = "ID"
		sql_escape = self.conn.escape if hasattr(self.conn, 'escape') else lambda st:repr(unicode(st))[1:]
		query_str = "UPDATE {} SET {} WHERE {}={}".format(tablename,_dict2sql(kw, ",", sql_escape), self._id_field_name, _id)
		self.retry_execute(query_str)
		self.conn.commit() # commits transaction to the SQL connection

class psqlClient(SQLBaseClient):
	"""pg8000 backend for connect to to PostgreSQL"""
	def __init__(self, **kw):
		super(self.__class__, self).__init__(**kw)
		from pg8000 import connect
		self.conn = connect(**self._kw)
		self._cursor = self.conn.cursor()
	def cursor(self):
		return self._cursor

SQLBaseClient.register(psqlClient)

class mysqlClient:
	"""pymysql backend for connect to to MySQL"""
	def __init__(self, **kw):
		super(self.__class__, self).__init__(**kw)
		kw = self._kw
		self.conn = connect(kw['host'],kw['user'], kw['password'], kw['database'], charset="utf8")
		self._cursor = self.conn.cursor()
	def cursor(self):
		return self._cursor
