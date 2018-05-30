# Apache Spark Examples

This project has examples using Apache Spark and Scala. I'd suggest reading the books [Agile Data Science](https://github.com/rjurney/Agile_Data_Code) or [Agile Data Science](https://github.com/rjurney/Agile_Data_Code_2) because of those examples were inspired by the book.

### What do you need to use these examples?
First of all, you need to download your emails. I used the python app in the [Agile Data Science GitHub](https://github.com/rjurney/Agile_Data_Code).
In that case I used this commands:
  - pip install lepl
  - pip install avro
  - My Gmail is in Portuguese, so I used this option for my All mail: '[Gmail]/Todo o correio' 

(optional) Gmail.py command:
```sh
$ ./gmail.py -m automatic -u 'your.email@gmail.com' -p 'your_password' -s ./email.avro.schema -f '[Gmail]/Todo o correio' -o /tmp/test_mbox 2>&1 &
```

