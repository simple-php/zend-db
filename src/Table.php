<?php

namespace SimplePHP\Zend_Db1;

use Zend_Db_Table_Abstract;
use Zend_Db_Table_Rowset_Abstract;
use Zend_Db_Table_Select;
use Zend_Db_Select;
use Zend_Db_Table_Row;
use Zend_Db_Table_Rowset;
use Exception;

/**
 *   Базовый класс для таблиц с использованием Zend_Db
 */
abstract class Table extends Zend_Db_Table_Abstract
{
    static $defaultSchema = '';

    function __construct($config = [])
    {
        if (!isset($this->_schema)) {
            $this->_schema = static::$defaultSchema;
        }
        parent::__construct($config);
    }

    protected static $instances;

    /**
     * @return self
     */
    public static function instance() {
        $class = get_called_class();

        return self::getInstance($class);
    }

    /**
     * @param $tableClassName
     * @return Table
     */
    public static function getInstance($tableClassName) {
        if (!isset(self::$instances[$tableClassName])) {
            self::$instances[$tableClassName] = new $tableClassName;
        }
        return self::$instances[$tableClassName];
    }

    /**
     * @return Zend_Db_Table_Select
     */
    public static function selectAll($as = null) {
        $table = static::instance();
        if ($as) {
            return $table->select(false)->from([$as => $table->_schema.'.'.$table->_name]);
        } else {
            return $table->select(true);
        }
    }

    /**
     * @param null $as
     * @return Select
     */
    public static function selectAs($as = null) {
        $table = static::instance();
        return new Select($table, $as);
    }

    /**
     * Возвращает массив со значениями поля идентификатора idField из набора записей
     * @param array|Zend_Db_Table_Rowset $rowset Набор записей
     * @param string $idField Поле идентификатора, по умолчанию 'id'
     * @return array Массив значений идентификатора из набора записей
     */
    public static function getIds($rowset, $idField = 'id') {
        $ids = [];
        foreach ($rowset as $r) {
            $ids[] = $r[$idField];
        }
        return $ids;
    }

    /**
     * Выполняет индексацию набора записей по id
     * Возвращает ассоциативный массив, где ключом является значение поля, указанного в idField
     * @param array|Zend_Db_Table_Rowset $rowset Набор записей
     * @param string $idField Поле по которому произвести индексацию, по умолчанию 'id'
     * @return array
     */
    public static function indexById($rowset, $idField = 'id', $valueField = null) {
        $arr = [];
        foreach ($rowset as $r) {
            $arr[$r[$idField]] = (isset($valueField) ?  $r[$valueField] :$r);
        }
        return $arr;
    }

    /**
     * Выполняет группировку набора записей по значениям указанного поля
     * Возвращает ассоциативный массив, где ключом является значение поля,
     * указанного в field, а значениями массива - массив записей
     * @param $rowset Набор записей
     * @param $field Название поля, по которому группировать записи
     * @return array
     */
    public static function groupBy($rowset, $field) {
        $arr = [];
        foreach ($rowset as $r) {
            if (!isset($arr[$r[$field]])) $arr[$r[$field]] = [];
            $arr[$r[$field]][] = $r;
        }
        return $arr;
    }

    /**
     * Возвращает запись по ID
     * @param $id
     * @return null|Zend_Db_Table_Row
     */
    static function getById($id) {
        return static::instance()->fetchRow(['id = ?' => $id]);
    }

    /**
     * Выполняет обновление записи по id
     * @param int $id Идентификатор записи
     * @param array $data Массив изменяемых полей
     * @return int Кол-во затронутых записей
     */
    static function updateById($id, $data) {
        return static::instance()->update($data, ['id = ?' => $id]);
    }

    /**
     * Выполняет выборку записей из таблицы.
     * Статическая обертка для fetchAll
     * @param array|string|Zend_Db_Table_Select $where Условие запроса
     * @param array|string $order Параметры сортировки
     * @param int $count Ограничение по макс. кол-ву записей в выборке
     * @param int $offset Смещение (кол-во записей в начале, которые будут пропущены) выборки
     * @return Zend_Db_Table_Rowset_Abstract
     */
    public static function fetchRows($where = null, $order = null, $count = null, $offset = null) {
        return self::instance()->fetchAll($where, $order, $count, $offset);
    }

    /**
     * Выполняет вставку новой записи
     * @param array $data Значения полей новой записи
     * @return mixed Первичный ключ вставленной записи
     */
    public static function insertRow($data) {
        return static::instance()->insert($data);
    }
}

class Select extends Zend_Db_Select {

    protected $_tables = [];

    /**
     * @param Table $table
     * @param $as
     */
    public function __construct($table, $as = null)
    {
        parent::__construct($table->getAdapter());
        $class = get_class($table);
        $this->_tables[$class] = $this->getCorrelationName($class, $as);
        $this->from($this->getSchemaAndTableName($table, $this->_tables[$class]), []);
        $tableName = $table->info(Zend_Db_Table_Abstract::SCHEMA).'.'.
            $table->info(Zend_Db_Table_Abstract::NAME);
        if (!isset($this->_tables[$tableName])) $this->_tables[$tableName] = $as;
    }

    public function getCorrelationName($tableClass, $as = null) {
        if (!$as) {
            if (isset($this->_tables[$tableClass])) {
                return $this->_tables[$tableClass];
            }
            /*else {
                $table = Table::getInstance($tableClass);
                return $table->as;
            }*/
        }
        return $as;
    }

    public function getSchemaAndTableName($table, $as = null) {
        $t = $table->info(Zend_Db_Table_Abstract::SCHEMA).'.'.
            $table->info(Zend_Db_Table_Abstract::NAME);
        if ($as) return [$as => $t]; else return $t;
    }

    public function joinLeftBy($reference, $as = null, $condition = null) {
        return $this->_joinBy($reference, Zend_Db_Select::LEFT_JOIN, $as, $condition);
    }

    public function joinRightBy($reference, $as = null, $condition = null) {
        return $this->_joinBy($reference, Zend_Db_Select::RIGHT_JOIN, $as, $condition);
    }

    public function joinFullBy($reference, $as = null, $condition = null) {
        return $this->_joinBy($reference, Zend_Db_Select::FULL_JOIN, $as, $condition);
    }

    public function joinBy($reference, $as = null, $condition = null) {
        return $this->_joinBy($reference, Zend_Db_Select::INNER_JOIN, $as, $condition);
    }

    public function _joinBy($reference, $joinType = Zend_Db_Select::INNER_JOIN, $as = null, $condition = null) {
        $ref = explode('=', $reference);
        if (count($ref) != 2) {
            $ref = explode('.', $reference);
            if (count($ref) != 2) throw new Exception('Invalid reference ' . $reference);
            $refTableClass = $ref[0];
            $refName = $ref[1];

            $refTable = Table::getInstance($refTableClass);

            $refMap = $refTable->info(Zend_Db_Table_Abstract::REFERENCE_MAP);

            if (!isset($refMap) || !isset($refMap[$refName]))
                throw new Exception('Reference ' . $reference . ' not found!');

            $ref = $refMap[$refName];

            if (isset($this->_tables[$refTableClass])) {
                $joinTableClass = $ref[Zend_Db_Table_Abstract::REF_TABLE_CLASS];
                $joinColumn = $ref[Zend_Db_Table_Abstract::REF_COLUMNS];
                $refColumn = $ref[Zend_Db_Table_Abstract::COLUMNS];
            } else {
                $joinTableClass = $refTableClass;
                $refTableClass = $ref[Zend_Db_Table_Abstract::REF_TABLE_CLASS];
                if (!isset($this->_tables[$refTableClass]))
                    throw new Exception('Reference ' . $reference . ' not connected with selected tables');
                $refColumn = $ref[Zend_Db_Table_Abstract::REF_COLUMNS];
                $joinColumn = $ref[Zend_Db_Table_Abstract::COLUMNS];
            }

            $joinTable = Table::getInstance($joinTableClass);

            /*if (!$as) {
                $as = $joinTable->as;
            }*/

            $joinTableName = $joinTable->info(Zend_Db_Table_Abstract::SCHEMA) . '.' .
                $joinTable->info(Zend_Db_Table_Abstract::NAME);

            $refTable = $refTableClass;
        } else {
            $left = $ref[0];
            $right = $ref[1];

            $leftDef = $this->_parseTableDef($left);
            $rightDef = $this->_parseTableDef($right);

            if (isset($this->_tables[$leftDef['table']])) {
                $joinTableName = $rightDef['table'];
                $joinColumn = $rightDef['column'];
                $refTable = $leftDef['table'];
                $refColumn = $leftDef['column'];
            } elseif (isset($this->_tables[$rightDef['table']])) {
                $joinTableName = $leftDef['table'];
                $joinColumn = $leftDef['column'];
                $refTable = $rightDef['table'];
                $refColumn = $rightDef['column'];
            }
        }

        if ($as) {
            $joinName = [$as => $joinTableName];
        } else {
            $joinName = $joinTableName;
        }

        $refAs = $this->getCorrelationName($refTable);
        if (!$refAs) {
            $refTable = Table::getInstance($refTable);
            $refAs = $refTable->info(Zend_Db_Table_Abstract::SCHEMA) . '.' .
                $refTable->info(Zend_Db_Table_Abstract::NAME);
        }

        $joinCond = $refAs . '.' . $refColumn . '=' . ($as ? $as : $joinTableName) . '.' . $joinColumn;

        if (!in_array($joinType, Zend_Db_Select::$_joinTypes)) $joinType = self::INNER_JOIN;
        if ($condition) {
            $joinCond .= ' AND '.$condition;
        }
        $this->_join($joinType, $joinName, $joinCond, []);

        if (isset($joinTableClass)) {
            if (!isset($this->_tables[$joinTableClass])) $this->_tables[$joinTableClass] = $as;
        }

        if (!isset($this->_tables[$joinTableName])) $this->_tables[$joinTableName] = $as;

        return $this;
    }

    protected function _parseTableDef($s) {
        $parts = explode('.', $s);

        if (count($parts) == 3) {
            $schema = $parts[0];
            $table = $parts[1];
            $column = $parts[2];
        } elseif (count($parts) == 2) {
            $schema = '';
            $table = $parts[0];
            $column = $parts[1];
        } else {
            throw new Exception('Invalid column definition for relation '.$s);
        }
        return [
            'table' => ($schema ? $schema.'.' : '').$table,
            'column' => $column
        ];
    }
}
