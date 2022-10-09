from gooey import Gooey, GooeyParser


@Gooey(program_name='sample program')
def main():

    main_parser = GooeyParser()
    main_parser.add_argument(
        'command', help='call a command', type=str, default='gui', nargs='?')
    main_parser.add_argument('--config', help='config file for command')
    main_args = main_parser.parse_args()

    if main_args.command == 'gui':
        print('launching gui program')
    elif main_args.command == 'app1':
        print(f'running app1 with config {main_args.config}')
    else:
        print(f'running app2 with config {main_args.config}')


if __name__ == '__main__':
    main()
