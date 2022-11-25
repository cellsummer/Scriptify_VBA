# from gooey import Gooey, GooeyParser
from utils.utils import logger, read_config


# @Gooey(program_name='sample program')
# def main():

#     logger.info('application started')
#     main_parser = GooeyParser()
#     main_parser.add_argument(
#         'command', help='call a command', type=str, default='gui', nargs='?')
#     main_parser.add_argument('--config', help='config file for command')
#     main_args = main_parser.parse_args()

#     if main_args.command == 'gui':
#         print('launching gui program')
#     elif main_args.command == 'app1':
#         print(f'running app1 with config {main_args.config}')
#     else:
#         print(f'running app2 with config {main_args.config}')

def test():
    logger.debug(f'Running {__file__}')

    config = read_config("configs/sample_config.json")
    logger.debug('config file loaded')
    logger.debug(config['server'])
    logger.debug(config['db'])
    logger.debug(config['table'])
    
    logger.debug(f'Finished running {__file__}')

if __name__ == '__main__':
    test()
