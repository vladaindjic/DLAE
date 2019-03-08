from parglare import Grammar, Parser

primer = """
    p:=int; 
    a:=double; 
    b:=double; 
    <asd := int(/\./)> </\..*/> <p>
    """


def main():
    g = Grammar.from_file('log_formatter.pg')
    p = Parser(g)
    res = p.parse(primer)
    print(res)


if __name__ == '__main__':
    main()
