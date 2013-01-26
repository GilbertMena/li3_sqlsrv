<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2012, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_sqlsrv\extensions\adapter\data\source\database;

use PDO;
use PDOStatement;
use PDOException;
use lithium\data\model\QueryException;

/**
 * Extends the `Database` class to implement the necessary SQL-formatting and resultset-fetching
 * features for working with MySQL databases.
 *
 * For more information on configuring the database connection, see the `__construct()` method.
 *
 * @see lithium\data\source\database\adapter\MySql::__construct()
 */
class SqlSrv extends \lithium\data\source\Database {

	/**
	 * @var PDO
	 */
	public $connection;

	protected $_classes = array(
		'entity' => 'lithium\data\entity\Record',
		'set' => 'lithium\data\collection\RecordSet',
		'relationship' => 'lithium\data\model\Relationship',
		'result' => 'li3_sqlsrv\extensions\adapter\data\source\database\sql_srv\Result'
	);

	/**
	 * MySQL column type definitions.
	 *
	 * @var array
	 */
	protected $_columns = array(
		'primary_key' => array('name' => 'IDENTITY (1, 1) NOT NULL'),
		'string' => array('name' => 'varchar', 'length' => '255'),
		'text' => array('name' => 'varchar', 'length' => 'max'),
		'integer' => array('name' => 'integer', 'length' => 11, 'formatter' => 'intval'),
		'float' => array('name' => 'float', 'formatter' => 'floatval'),
		'datetime' => array('name' => 'datetime', 'format' => 'Y-m-d H:i:s.u', 'formatter' => 'date'),
		'timestamp' => array('name' => 'timestamp', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
		'time' => array('name' => 'datetime', 'format' => 'H:i:s', 'formatter' => 'date'),
		'date'  => array('name' => 'datetime', 'format' => 'Y-m-d', 'formatter' => 'date'),
		'binary' => array('name' => 'varbinary', 'length' => 'max'),
		'boolean' => array('name' => 'bit')
	);

	/**
	 * Pair of opening and closing quote characters used for quoting identifiers in queries.
	 *
	 * @var array
	 */
	protected $_quotes = array('[', ']');

	/**
	 * MySQL-specific value denoting whether or not table aliases should be used in DELETE and
	 * UPDATE queries.
	 *
	 * @var boolean
	 */
	protected $_useAlias = true;

	/**
	 * Constructs the MySQL adapter and sets the default port to 3306.
	 *
	 * @see lithium\data\source\Database::__construct()
	 * @see lithium\data\Source::__construct()
	 * @see lithium\data\Connections::add()
	 * @param array $config Configuration options for this class. For additional configuration,
	 *        see `lithium\data\source\Database` and `lithium\data\Source`. Available options
	 *        defined by this class:
	 *        - `'database'`: The name of the database to connect to. Defaults to 'lithium'.
	 *        - `'host'`: The IP or machine name where MySQL is running, followed by a colon,
	 *          followed by a port number or socket. Defaults to `'localhost:3306'`.
	 *        - `'persistent'`: If a persistent connection (if available) should be made.
	 *          Defaults to true.
	 *
	 * Typically, these parameters are set in `Connections::add()`, when adding the adapter to the
	 * list of active connections.
	 */
	public function __construct(array $config = array()) {
		$defaults = array('host' => 'localhost:1433', 'encoding' => null);
		parent::__construct($config + $defaults);
	}

	/**
	 * Check for required PHP extension, or supported database feature.
	 *
	 * @param string $feature Test for support for a specific feature, i.e. `"transactions"` or
	 *               `"arrays"`.
	 * @return boolean Returns `true` if the particular feature (or if MySQL) support is enabled,
	 *         otherwise `false`.
	 */
	public static function enabled($feature = null) {
		if (!$feature) {
			return extension_loaded('pdo_odbc');
		}
		$features = array(
			'arrays' => false,
			'transactions' => false,
			'booleans' => true,
			'relationships' => true
		);
		return isset($features[$feature]) ? $features[$feature] : null;
	}

	/**
	 * Connects to the database using the options provided to the class constructor.
	 *
	 * @return boolean Returns `true` if a database connection could be established, otherwise
	 *         `false`.
	 */
	public function connect() {
		$config = $this->_config;
		$this->_isConnected = false;
		$host = $config['host'];

		if (!$config['database']) {
			return false;
		}

		$options = array(
			PDO::ATTR_PERSISTENT => $config['persistent'],
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
		);

		try {
			list($host, $port) = array(1 => "1433") + explode(':', $host);
			$dsn = sprintf("odbc:Driver=SQL Server Native Client 11.0; Server=%s; Port=%s; Database=%s; UID=%s; PWD=%s;", $host, $port, $config['database'],$config['login'],$config['password']);
			//echo "\r\n".'the dsn: '.$dsn; //exit;
			//$this->connection = new PDO($dsn, $options);
			$this->connection = new PDO($dsn);
			//var_dump($this->connection);
		} catch (PDOException $e) {
			$error = (string) $e;
			$code = 1;
			throw new QueryException("{$error}", $code);
			return false;
		}

		$this->_isConnected = true;

		if ($config['encoding']) {
			$this->encoding($config['encoding']);
		}
//var_dump($this->connection);exit;
		//$info = $this->connection->getAttribute(PDO::ATTR_SERVER_VERSION);

		//$this->_useAlias = (boolean) version_compare($info, "4.1", ">=");
		return $this->_isConnected;
	}

	/**
	 * Disconnects the adapter from the database.
	 *
	 * @return boolean True on success, else false.
	 */
	public function disconnect() {
		if ($this->_isConnected) {
			unset($this->connection);
			$this->_isConnected = false;
			return true;
		}
		return true;
	}

	/**
	 * Returns the list of tables in the currently-connected database.
	 *
	 * @param string $model The fully-name-spaced class name of the model object making the request.
	 * @return array Returns an array of sources to which models can connect.
	 * @filter This method can be filtered.
	 */
	public function sources($model = null) {
		return $this->_filter(__METHOD__, compact('model'), function($self, $params) {
			$query = "SELECT TABLE_NAME FROM [INFORMATION_SCHEMA].[TABLES]";

			if (!$result = $self->invokeMethod('_execute', array($query))) {
				return null;
			}
			$entities = array();

			while ($data = $result->next()) {
				$entities[] = $data['TABLE_NAME'];
			}
			return $entities;
		});
	}

	/**
	 * Gets the column schema for a given MySQL table.
	 *
	 * @param mixed $entity Specifies the table name for which the schema should be returned, or
	 *        the class name of the model object requesting the schema, in which case the model
	 *        class will be queried for the correct table name.
	 * @param array $meta
	 * @return array Returns an associative array describing the given table's schema, where the
	 *         array keys are the available fields, and the values are arrays describing each
	 *         field, containing the following keys:
	 *         - `'type'`: The field type name
	 * @filter This method can be filtered.
	 */
	public function describe($entity, array $meta = array()) {
		$params = compact('entity', 'meta');
		return $this->_filter(__METHOD__, $params, function($self, $params, $chain) {
			extract($params);

			$name = $self->invokeMethod('_entityName', array($entity));
			$sql = "SELECT COLUMN_NAME as Field, DATA_TYPE as Type, "
				. "COL_LENGTH('{$name}', COLUMN_NAME) as Length, IS_NULLABLE As [Null], "
				. "COLUMN_DEFAULT as [Default], "
				. "COLUMNPROPERTY(OBJECT_ID('{$name}'), COLUMN_NAME, 'IsIdentity') as [Key], "
				. "NUMERIC_SCALE as Size FROM INFORMATION_SCHEMA.COLUMNS "
				. "WHERE TABLE_NAME = '{$name}'";

			if (!$columns = $self->invokeMethod('_execute', array($sql))) {
				return null;
			}
			$fields = array();

			while ($column = $columns->next()) {
				$fields[$column['Field']] = array(
					'type'     => $column['Type'],
					'length'   => $column['Length'],
					'null'     => ($column['Null'] == 'YES' ? true : false),
					'default'  => $column['Default'],
					'key'      => ($column['Key'] == 1 ? 'primary' : null)
				);
			}
			return $fields;
		});
	}

	/**
	 * Gets or sets the encoding for the connection.
	 *
	 * @param $encoding
	 * @return mixed If setting the encoding; returns true on success, else false.
	 *         When getting, returns the encoding.
	 */
	public function encoding($encoding = null) {
		return true;
	}
    
    public function create($query, array $options = array()) {
		if (is_object($query)) {
			$table = $query->source();
			$this->_execute("Set IDENTITY_INSERT [dbo].[{$table}] On");
		}
		return parent::create($query, $options);
	}

	/**
	 * Converts a given value into the proper type based on a given schema definition.
	 *
	 * @see lithium\data\source\Database::schema()
	 * @param mixed $value The value to be converted. Arrays will be recursively converted.
	 * @param array $schema Formatted array from `lithium\data\source\Database::schema()`
	 * @return mixed Value with converted type.
	 */
	public function value($value, array $schema = array()) {
		if (($result = parent::value($value, $schema)) !== null) {
			return $result;
		}
		return "'" . $value . "'";
	}

	/**
	 * In cases where the query is a raw string (as opposed to a `Query` object), to database must
	 * determine the correct column names from the result resource.
	 *
	 * @param mixed $query
	 * @param resource $resource
	 * @param object $context
	 * @return array
	 */
	public function schema($query, $resource = null, $context = null) {
		if (is_object($query)) {
			return parent::schema($query, $resource, $context);
		}

		$result = array();
		$count = $resource->resource()->columnCount();

		for ($i = 0; $i < $count; $i++) {
			$meta = $resource->resource()->getColumnMeta($i);
			$result[] = $meta['name'];
		}
		return $result;
	}

	/**
	 * Retrieves database error message and error code.
	 *
	 * @return array
	 */
	public function error() {
		if ($error = $this->connection->errorInfo()) {
			return array($error[1], $error[2]);
		}
		return null;
	}

	public function alias($alias, $context) {
		if ($context->type() == 'update' || $context->type() == 'delete') {
			return;
		}
		return parent::alias($alias, $context);
	}

	/**
	 * @todo Eventually, this will need to rewrite aliases for DELETE and UPDATE queries, same with
	 *       order().
	 * @param string $conditions
	 * @param string $context
	 * @param array $options
	 * @return void
	 */
	public function conditions($conditions, $context, array $options = array()) {
		return parent::conditions($conditions, $context, $options);
	}

	/**
	 * Execute a given query.
 	 *
 	 * @see lithium\data\source\Database::renderCommand()
	 * @param string $sql The sql string to execute
	 * @param array $options Available options:
	 *        - 'buffered': If set to `false` uses mysql_unbuffered_query which
	 *          sends the SQL query query to MySQL without automatically fetching and buffering the
	 *          result rows as `mysql_query()` does (for less memory usage).
	 * @return resource Returns the result resource handle if the query is successful.
	 * @filter
	 */
	protected function _execute($sql, array $options = array()) {
		
		//echo 'sql: '."\r\n";
		//echo $sql;
		//echo "\r\n".'the db: '."\r\n";
		//echo $this->_config['database']; 
		$defaults = array('buffered' => true);
		$options += $defaults;
		
		//echo "\r\n".'isConnected:'."\r\n";
		//var_dump($this->_isConnected);
		
		if(!$this->_isConnected)
		{
			$this->connect();
		}
		
		//echo "\r\n".'the connection:';
		//var_dump($this->connection);
		
		$this->connection->exec("USE  `{$this->_config['database']}`");
		
		//echo "\r\n".'past the db select'."\r\n";
		//exit;
		
		$conn = $this->connection;

		$params = compact('sql', 'options');

		return $this->_filter(__METHOD__, $params, function($self, $params) use ($conn) {
			$sql = $params['sql'];
			$options = $params['options'];
			//$statement = $db->prepare($query);
			//$statement->execute();
			//echo "\r\n".' The Sql in the filter: '.$sql; //exit;
			if (!($resource = $conn->query($sql)) instanceof PDOStatement) {
				list($code, $error) = $self->error();
				throw new QueryException("{$sql}: {$error}", $code);
			}
			//echo "\r\n".' The resource';
			//print_r($resource); //exit;
			
			return $self->invokeMethod('_instance', array('result', compact('resource')));
		});
	}
	
	//returns the resultset from an stored proc
	public function executeStoredProc($sql, array $options = array()) {
		
		//echo 'sql: '."\r\n";
		//echo $sql;
		//echo "\r\n".'the db: '."\r\n";
		//echo $this->_config['database']; 
		$defaults = array('buffered' => true);
		$options += $defaults;
		
		//echo "\r\n".'isConnected:'."\r\n";
		//var_dump($this->_isConnected);
		
		if(!$this->_isConnected)
		{
			$this->connect();
		}
		
		//echo "\r\n".'the connection:';
		//var_dump($this->connection);
		
		$this->connection->exec("USE  `{$this->_config['database']}`");
		
		//echo "\r\n".'past the db select'."\r\n";
		//exit;
		
		$conn = $this->connection;

		$params = compact('sql', 'options');

		return $this->_filter(__METHOD__, $params, function($self, $params) use ($conn) {
			$sql = $params['sql'];
			$options = $params['options'];
			//$statement = $db->prepare($query);
			//$statement->execute();
			//echo "\r\n".' The Sql in the filter: '.$sql; //exit;
			if (!($resource = $conn->query($sql)) instanceof PDOStatement) {
				list($code, $error) = $self->error();
				throw new QueryException("{$sql}: {$error}", $code);
			}
			//echo "\r\n".' The resource'."\r\n";
			//print_r($resource); //exit;
			$finalResult = $resource->fetchAll(PDO::FETCH_ASSOC);
			return $finalResult;
			//echo "\r\n".' The finalResult: '."\r\n";
			//print_r($finalResult);
			//exit;
			
			//return $self->invokeMethod('_instance', array('result', compact('resource')));
		});
	}

	protected function _results($results) {
		/* @var $results PDOStatement */
		$numFields = $results->columnCount();
		$index = $j = 0;

		while ($j < $numFields) {
			$column = $results->getColumnMeta($j);
			$name = $column['name'];
			$table = $column['table'];
			$this->map[$index++] = empty($table) ? array(0, $name) : array($table, $name);
			$j++;
		}
	}

	/**
	 * Gets the last auto-generated ID from the query that inserted a new record.
	 *
	 * @param object $query The `Query` object associated with the query which generated
	 * @return mixed Returns the last inserted ID key for an auto-increment column or a column
	 *         bound to a sequence.
	 */
	protected function _insertId($query) {
		$resource = $this->_execute('SELECT @@identity as insertId');
		$id = $this->result('next', $resource, null);
		$id = $id['insertId'];
		$this->result('close', $resource, null);

		if (!empty($id) && $id !== '0') {
			return $id;
		}
	}

	/**
	 * Converts database-layer column types to basic types.
	 *
	 * @param string $real Real database-layer column type (i.e. `"varchar(255)"`)
	 * @return array Column type (i.e. "string") plus 'length' when appropriate.
	 */
	protected function _column($real) {

		if (is_array($real)) {
			$length = '';
			if (isset($real['length'])) {
				$length = $real['length'];
				if ($length === -1) {
					$length = 'max';
				}
				$length = '(' . $length . ')';
			}
			return $real['type'] . $length;
		}

		if (!preg_match('/(?P<type>[^(]+)(?:\((?P<length>[^)]+)\))?/', $real, $column)) {
			return $real;
		}
		$column = array_intersect_key($column, array('type' => null, 'length' => null));

		switch (true) {
			case $column['type'] === 'datetime':
			break;
			case ($column['type'] == 'tinyint' && $column['length'] == '1'):
			case ($column['type'] == 'bit'):
				$column = array('type' => 'boolean');
			break;
			case (strpos($column['type'], 'int') !== false):
				$column['type'] = 'integer';
			break;
			case (strpos($column['type'], 'text') !== false):
				$column['type'] = 'text';
			break;
			case strpos($column['type'], 'char') !== false:
				if (isset($column['length']) && $column['length'] === 'max') {
					$column['type'] = 'text';
					unset($column['length']);
				} else {
					$column['type'] = 'string';
				}
			break;
			case (strpos($column['type'], 'binary') !== false || $column['type'] == 'image'):
				$column['type'] = 'binary';
			break;
			case preg_match('/float|double|decimal/', $column['type']):
				$column['type'] = 'float';
			break;
			default:
				$column['type'] = 'text';
			break;
		}
		return $column;
	}
    
    /**
	 * Helper method that retrieves an entity's name via its metadata.
	 *
	 * @param string $entity Entity name.
	 * @return string Name.
	 */
	protected function _entityName($entity) {
		if (class_exists($entity, false) && method_exists($entity, 'meta')) {
			$entity = $entity::meta('name');
		}
		return $entity;
	}
}

?>