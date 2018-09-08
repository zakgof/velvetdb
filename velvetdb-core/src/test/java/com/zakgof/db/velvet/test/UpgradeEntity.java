package com.zakgof.db.velvet.test;

public class UpgradeEntity {

    public static class Original {
        public String strf;
        public int intf;
        public Integer integerf;
    }

    public static class RemovedField {
        public String strf;
        public Integer integerf;
    }

    public static class AddedField {
        public String strf;
        public int intf;
        public String[] addedArray;
        public Integer integerf;
    }

    public static class RenamedField {
        public String strf;
        public int intfWithNewName;
        public Integer integerf;
    }

    public static class FieldChangedType {
        public int[] strf;
        public int intf;
        public Integer integerf;
    }

    public static class ChangeFieldOrder {
        public Integer integerf;
        public int intf;
        public String strf;
    }
}
