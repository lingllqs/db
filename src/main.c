#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/db.h"

int main(int argc, char *argv[])
{
    InputBuffer *input_buffer = new_input_buffer();
    if (argc < 2)
    {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = db_open(filename);
    while (true)
    {
        /* 打印提示符 */
        print_prompt();

        /* 读取输入 */
        read_input(input_buffer);

        /* 处理元命令 */
        if (input_buffer->buffer[0] == '.')
        {
            MetaCommandResult ret = do_meta_command(input_buffer, table);
            switch (ret)
            {
            case META_COMMAND_SUCCESS:
                continue;
            case META_COMMAND_UNRECOGNIZE_COMMAND:
                printf("Unrecognize meta command: '%s'\n", input_buffer->buffer);
                continue;
            }
        }

        /* 检查语句是否合法 */
        Statement statement;
        PrepareResult prepare_result = prepare_statement(input_buffer, &statement);
        switch (prepare_result)
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error: Cound not parse statement.\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("String is too long.\n");
            continue;
        case PREPARE_NEGATIVE_ID:
            printf("ID must be positive.\n");
            continue;
        case PREPARE_UNRECOGNIZE_STATEMENT:
            printf("Unrecognize statement at start of '%s'\n", input_buffer->buffer);
            continue;
        }

        /* 执行语句 */
        switch (execute_statement(&statement, table))
        {
        case EXECUTE_SUCCESS:
            printf("Executed!\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Error: table full\n");
            break;
        }
    }

    return EXIT_SUCCESS;
}
