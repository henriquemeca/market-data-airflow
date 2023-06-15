from argparse import ArgumentParser, Namespace
from typing import List


class ArgumentConiguration(object):
    """
    Parses arguments for configuring an ETL pipeline
    """

    def __init__(self, required_args: List[str], extra: List[str] = []) -> None:
        parser: ArgumentParser = ArgumentParser(
            description="Input arguments for pipelines to run correctly"
        )

        self.__set_parameter(parser=parser, arguments=required_args, required=True)
        self.__set_parameter(parser=parser, arguments=extra)

        args, _ = parser.parse_known_args()

        self.__set_attribute(args=args, variables=required_args)
        self.__set_attribute(args=args, variables=extra)

    def __set_parameter(
        self, parser: ArgumentParser, arguments: List[str], required: bool = False
    ):
        for argument in arguments:
            argument_name = f"--{argument.replace('_', '-')}"
            kwargs = {
                "dest": argument,
                "type": str,
                "required": required,
            }

            print(argument_name)
            parser.add_argument(argument_name, **kwargs)

    def __set_attribute(self, args: Namespace, variables: List[str]):
        for variable in variables:
            setattr(self, variable, getattr(args, variable))
