#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

typedef struct InputBuffer
{
    char *buffer;         // 用户输入
    size_t buffer_length; // buffer 长度
    ssize_t input_length;
} InputBuffer;

typedef enum MetaCommandResult
{
    META_COMMAND_SUCCESS,            // 元命令合法
    META_COMMAND_UNRECOGNIZE_COMMAND // 元命令不存在
} MetaCommandResult;

typedef enum PrepareResult
{
    PREPARE_SUCCESS,               // 语法正确
    PREPARE_SYNTAX_ERROR,          // 语法错误
    PREPARE_UNRECOGNIZE_STATEMENT, // 不能识别
    PREPARE_STRING_TOO_LONG,       // 字符串过长
    PREPARE_NEGATIVE_ID            // ID 为负数

} PrepareResult;

typedef enum ExecuteResult
{
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS
} ExecuteResult;

/* 语句类型 */
typedef enum StatementType
{
    STATEMENT_INSERT, // 插入
    STATEMENT_SELECT  // 查询
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct Row
{
    uint32_t id;                             // ID
    char username[COLUMN_USERNAME_SIZE + 1]; // 用户名
    char email[COLUMN_EMAIL_SIZE + 1];       // 邮箱
} Row;

typedef struct Statement
{
    StatementType type; // 语句类型
    Row row_to_insert;  // 插入语句
} Statement;

#define size_of_attribute(Type, Member) sizeof(((Type *)0)->Member)

#define TABLE_MAX_PAGES 100

typedef struct Pager
{
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct Table
{
    uint32_t num_rows;
    Pager *pager;
} Table;

/* 定义一个指针来记录读取位置 */
typedef struct Cursor
{
    Table *table;
    uint32_t row_num;
    bool end_of_table;
} Cursor;

InputBuffer *new_input_buffer();
void print_prompt();
void read_input(InputBuffer *input_buffer);
void free_input_buffer(InputBuffer *input_buffer);

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);

void print_row(Row *row);
void *row_slot(Table *table, uint32_t row_num);
void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);
Table *db_open(const char *filename);
Pager *pager_open(const char *filename);
void db_close(Table *table);
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size);
void *get_page(Pager *pager, uint32_t page_num);
/*void free_table(Table *table);*/

ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
ExecuteResult execute_statement(Statement *statement, Table *table);

#endif // !DB_H
