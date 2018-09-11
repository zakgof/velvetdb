package com.zakgof.db.velvet.test;

class Original {
    public String strf;
    public int intf;
    public Integer integerf;
}

class RemovedField {
    public String strf;
    public Integer integerf;
}

class AddedField {
    public String strf;
    public int intf;
    public String[] addedArray;
    public Integer integerf;
}

class RenamedField {
    public String strf;
    public int intfWithNewName;
    public Integer integerf;
}

class FieldChangedType {
    public int[] strf;
    public int intf;
    public Integer integerf;
}

class ChangeFieldOrder {
    public Integer integerf;
    public int intf;
    public String strf;
}

enum SomeEnum {
}

enum OriginalEnum {
    One,
    Two,
    Three,
    Four
}

enum EnumLabelInserted {
    One,
    Two,
    Two222,
    Three,
    Four
}

enum EnumLabelRemoved {
    One,
    Three,
    Four
}

enum EnumLabelReordered {
    Four,
    Three,
    Two,
    One
}
