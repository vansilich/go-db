# Freelist

## Intro
Freelist is page-based linked list also known as `unrolled linked list`

## Scheme
Each node (`LNode` struct in code) have format:
```
| next | pointers | unused |
|  8B  |   n*8B   |   ...  |
```

Whole list scheme:
```
                     first_item
                         ↓
head_page -> [ next |    xxxxx ]
                ↓
             [ next | xxxxxxxx ]
                ↓
tail_page -> [ NULL | xxxx     ]
                         ↑
                     last_item
```