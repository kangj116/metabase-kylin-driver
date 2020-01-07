(ns metabase.driver.kylin
  "Driver for Kylin databases"
  (:require [clojure.java.jdbc :as jdbc]
            [jdbc.pool.c3p0 :as pool]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [sync :as sql-jdbc.sync]
             [execute :as sql-jdbc.exec]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.util
             [honeysql-extensions :as hx]]
            [clojure.tools.logging :as log]
            [metabase.util :as u]
            [metabase.connection-pool :as connection-pool]
            [metabase.mbql.util :as mbql.u]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor.util :as qputil])
  (:import [java.sql DatabaseMetaData]))

(def db-spec {:classname   "org.apache.kylin.jdbc.Driver"
              :subprotocol "kylin"
              :subname     "//10.128.3.51:7070/udian_chuxing"
              :user        "ADMIN"
              :password    "KYLIN"})

(def ^:private connection-pool-properties
  {"maxIdleTime"     (* 3 60 60)
   "minPoolSize"     1
   "initialPoolSize" 1
   "maxPoolSize"     15})

(def pool-spec (connection-pool/connection-pool-spec db-spec connection-pool-properties))

(driver/register! :kylin, :parent :sql-jdbc)

(defmethod sql-jdbc.conn/connection-details->spec :kylin
  [_ {:keys [user password dbname host port]
      :or   {user "ADMIN", password "KYLIN", dbname "", host "localhost", port "7070"}
      :as   details}]
  (-> {:classname   "org.apache.kylin.jdbc.Driver"
       :subprotocol "kylin"
       :subname     (str "//" host ":" port "/" dbname)
       :password    password
       :user        user}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :url)))

(defn- get-tables
  [^DatabaseMetaData md, ^String schema-or-nil, ^String db-name-or-nil]
  (vec (jdbc/metadata-result (.getTables md db-name-or-nil schema-or-nil "%" (into-array ["TABLE"])))))

(defn- post-filtered-active-tables
  [driver, ^DatabaseMetaData md, & [db-name-or-nil]]
  (set (for [table (filter #(not (contains? (sql-jdbc.sync/excluded-schemas driver) (:table_schem %)))
                           (get-tables md db-name-or-nil nil))]
         (let [remarks (:remarks table)]
           {:name        (:table_name table)
            :schema      (:table_schem table)
            :description (when-not (str/blank? remarks) remarks)}))))

(defmethod driver/describe-database :kylin
  [driver _]
  (jdbc/with-db-metadata [md pool-spec]
                         {:tables (post-filtered-active-tables driver md)}))

(defmethod driver/describe-table :kylin [driver _ table]
  (jdbc/with-db-metadata [md pool-spec]
                         (->>
                           (assoc (select-keys table [:name :schema]) :fields (sql-jdbc.sync/describe-table-fields md driver table))
                           (sql-jdbc.sync/add-table-pks md))))

(defn- sub-type-name
  [type_name]
  (def pos (.indexOf (name type_name) "("))
  (if (> pos 0)
    (keyword (subs (name type_name) 0 pos))
    type_name))

(def ^:private default-base-types
  {:BIGINT     :type/BigInteger
   :BINARY     :type/*
   :BIT        :type/Boolean
   :BLOB       :type/*
   :CHAR       :type/Text
   :DATE       :type/Date
   :DATETIME   :type/DateTime
   :DECIMAL    :type/Decimal
   :DOUBLE     :type/Float
   :ENUM       :type/*
   :FLOAT      :type/Float
   :INT        :type/Integer
   :INTEGER    :type/Integer
   :LONGBLOB   :type/*
   :LONGTEXT   :type/Text
   :MEDIUMBLOB :type/*
   :MEDIUMINT  :type/Integer
   :MEDIUMTEXT :type/Text
   :NUMERIC    :type/Decimal
   :REAL       :type/Float
   :SET        :type/*
   :SMALLINT   :type/Integer
   :TEXT       :type/Text
   :TIME       :type/Time
   :TIMESTAMP  :type/DateTimeWithLocalTZ
   :TINYBLOB   :type/*
   :TINYINT    :type/Integer
   :TINYTEXT   :type/Text
   :VARBINARY  :type/*
   :VARCHAR    :type/Text
   :YEAR       :type/Integer})

(defmethod sql-jdbc.sync/database-type->base-type :kylin
  [_ column]
  (def sub-column (sub-type-name column))
  (default-base-types sub-column))

(defmethod driver/describe-table-fks :kylin
  [_ _ table & [^String db-name-or-nil]]
  (jdbc/with-db-metadata [md pool-spec]
                         (with-open [rs (.getImportedKeys md db-name-or-nil, ^String (:schema table), ^String (:name table))]
                           (set
                             (for [result (jdbc/metadata-result rs)]
                               {:fk-column-name   (:fkcolumn_name result)
                                :dest-table       {:name   (:pktable_name result)
                                                   :schema (:pktable_schem result)}
                                :dest-column-name (:pkcolumn_name result)})))))

(defmethod sql.qp/quote-style :kylin [_] :mysql)

(defmethod driver/supports? [:kylin :full-join] [_ _] false)
(defmethod driver/supports? [:kylin :right-join] [_ _] false)
(defmethod driver/supports? [:kylin :set-timezone] [_ _] false)

(defmethod sql.qp/unix-timestamp->timestamp [:kylin :seconds] [_ _ expr]
  (hsql/call :from_unixtime expr))

(defn- date-format [format-str expr] (hsql/call :date_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :str_to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:kylin :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:kylin :minute]          [_ _ expr] (trunc-with-format "%Y-%m-%d %H:%i" expr))
(defmethod sql.qp/date [:kylin :minute-of-hour]  [_ _ expr] (hx/minute expr))
(defmethod sql.qp/date [:kylin :hour]            [_ _ expr] (trunc-with-format "%Y-%m-%d %H" expr))
(defmethod sql.qp/date [:kylin :hour-of-day]     [_ _ expr] (hx/hour expr))
(defmethod sql.qp/date [:kylin :day]             [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:kylin :day-of-week]     [_ _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:kylin :day-of-month]    [_ _ expr] (hsql/call :dayofmonth expr))
(defmethod sql.qp/date [:kylin :day-of-year]     [_ _ expr] (hsql/call :dayofyear expr))
(defmethod sql.qp/date [:kylin :month-of-year]   [_ _ expr] (hx/month expr))
(defmethod sql.qp/date [:kylin :quarter-of-year] [_ _ expr] (hx/quarter expr))
(defmethod sql.qp/date [:kylin :year]            [_ _ expr] (hsql/call :makedate (hx/year expr) 1))

(defmethod sql.qp/date [:kylin :week] [_ _ expr]
  (str-to-date "%X%V %W"
               (hx/concat (hsql/call :yearweek expr)
                          (hx/literal " Sunday"))))

(defmethod sql.qp/date [:kylin :week-of-year] [_ _ expr]
  (hx/inc (hx/week expr 6)))

(defmethod sql.qp/date [:kylin :month] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (hx/concat (date-format "%Y-%m" expr)
                          (hx/literal "-01"))))

(defmethod sql.qp/date [:kylin :quarter] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (hx/concat (hx/year expr)
                          (hx/literal "-")
                          (hx/- (hx/* (hx/quarter expr)
                                      3)
                                2)
                          (hx/literal "-01"))))

(defmethod driver/date-add :kylin
  [_ dt amount unit]
  (hsql/call :date_add dt (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

(defmethod driver/execute-query :kylin
  [driver {:keys [_ _ max-rows], query :native}]
  (let [sql              (str/replace (:query query) "`" "")
        [columns & rows] (jdbc/query pool-spec sql
                                     {:identifiers    identity
                                      :as-arrays?     true
                                      :read-columns   (partial sql-jdbc.exec/read-columns driver)
                                      :set-parameters (partial sql-jdbc.exec/set-parameters driver)
                                      :max-rows       max-rows})]
    {:rows    (or rows [])
     :columns (map u/qualified-name columns)})
  )
