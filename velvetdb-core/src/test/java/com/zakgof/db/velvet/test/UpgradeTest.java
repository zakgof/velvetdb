package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.IFixer;
import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader.Mode;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.Remapper;

@Ignore
public class UpgradeTest extends AVelvetTest {

    private Original orig;

    @Before
    public void setupData() {
        env.upgrader().clear();
        orig = new Original();
        orig.strf = "HELLO";
        orig.intf = 4567;
        orig.integerf = 1234;
    }

    @Test
    public void testRemoveField() {
        RemovedField rf = upgrade(orig, RemovedField.class);
        Assert.assertEquals("HELLO", rf.strf);
        Assert.assertTrue(1234 == rf.integerf.intValue());
    }

    @Test
    public void testAddField() {
        AddedField e = upgrade(orig, AddedField.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intf);
        Assert.assertNull(e.addedArray);
    }

    @Test
    @Ignore
    public void testRenameField() {
        RenamedField e = upgrade(orig, RenamedField.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(0 == e.intfWithNewName);
    }

    @Test
    public void testChangeFieldOrder() {
        ChangeFieldOrder e = upgrade(orig, ChangeFieldOrder.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intf);
    }

    @Test
    public void testFieldChangedType() {
        FieldChangedType e = upgrade(orig, FieldChangedType.class);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intf);
        Assert.assertNull(e.strf);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testFixer() {
        IFixer<Object> fixer = (Object ent, Map<String, Object> fields) -> {
            RenamedField rf = DynInstance.wrap(ent).as(RenamedField.class);
            rf.intfWithNewName = (Integer)fields.get("intf");
            return DynInstance.create(ent.getClass(), rf).instance();
        };

        Upgraderr<Original, RenamedField> upgraderr = new Upgraderr<>(Original.class, RenamedField.class);
        env.upgrader().fix((Class)upgraderr.clazz2, fixer);
        upgraderr.saveOriginal(orig);
        RenamedField e = upgraderr.readUpgraded();

        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intfWithNewName);
    }

    @Test
    public void testEnumRemoveLabel() {
        String restored = upgradeEnum(EnumLabelRemoved.class, OriginalEnum.Three);
        Assert.assertSame(restored, OriginalEnum.Three.name());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnumRemoveOurLabel() {
        upgradeEnum(EnumLabelRemoved.class, OriginalEnum.Two);
    }

    @Test
    public void testEnumInsertLabel() {
        String restored = upgradeEnum(EnumLabelInserted.class, OriginalEnum.Three);
        Assert.assertSame(restored, OriginalEnum.Three.name());
    }

    @Test
    public void testEnumReorderLabel() {
        String restored = upgradeEnum(EnumLabelReordered.class, OriginalEnum.Three);
        Assert.assertSame(restored, OriginalEnum.Three.name());
    }

    private String upgradeEnum(Class<?> prototype, OriginalEnum value) {
        return upgradeEnum(prototype, value, null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private String upgradeEnum(Class<?> prototype, OriginalEnum value, IFixer<Enum<?>> fixer) {
        env.upgrader().trackPackages(Mode.UPGRADE_ON_WRITE, "com.zakgof.db.velvet.test");
        IKeylessEntityDef entity1 = Entities.keyless(OriginalEnum.class);
        Object key = env.calculate(velvet -> entity1.put(velvet, value));

        Class<?> en2 = cloneClass(prototype, OriginalEnum.class.getName());

        if (fixer != null) {
            env.upgrader().fix(en2, (IFixer)fixer);
        }

        IKeylessEntityDef entity2 = Entities.keyless(en2);
        Object restored = env.calculate(velvet -> entity2.get(velvet, key));
        return restored.toString();
    }

    @SuppressWarnings({ "unchecked" })
    private <O, N> N upgrade(O originalValue, Class<N> newClass) {
        Upgraderr<O, N> upgraderr = new Upgraderr<>((Class<O>)originalValue.getClass(), newClass);
        upgraderr.saveOriginal(originalValue);
        return upgraderr.readUpgraded();
    }

    private class Upgraderr<O, N> {

        private Class<?> clazz1;
        private Class<?> clazz2;

        private Object key;
        private Class<N> newClass;

        public Upgraderr(Class<O> originalClass, Class<N> newClass) {
            this.newClass = newClass;
            this.clazz1 = cloneClass(originalClass,  newClass.getName());
            env.upgrader().trackPackages(Mode.UPGRADE_ON_WRITE, "com.zakgof.db.velvet.test");

            this.clazz2 = cloneClass(newClass,  newClass.getName());
        }

        @SuppressWarnings("unchecked")
        private N readUpgraded() {
            fixObjenesisCache();
            @SuppressWarnings("rawtypes")
            IKeylessEntityDef entity2 = Entities.keyless(clazz2);
            Object result2 = env.calculate(velvet -> entity2.get(velvet, key));
            DynInstance wrappedResult2 = DynInstance.wrap(result2);
            N deflated = wrappedResult2.as(newClass);
            return deflated;
        }

        @SuppressWarnings("unchecked")
        public void saveOriginal(O originalValue) {
            fixObjenesisCache();
            @SuppressWarnings("rawtypes")
            IKeylessEntityDef entity1 = Entities.keyless(clazz1);
            DynInstance i1 = DynInstance.create(clazz1, originalValue);
            key = env.calculate(velvet -> entity1.put(velvet, i1.instance()));
        }
    }

    @Before
    public void fixObjenesisCache() {
//        try {
//            Field field = (ZeSerializer.class).getDeclaredField("objenesis");
//            field.setAccessible(true);
//            Field modifiersField = Field.class.getDeclaredField("modifiers");
//            modifiersField.setAccessible(true);
//            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
//            field.set(null, new ObjenesisStd(false));
//        } catch (ReflectiveOperationException e) {
//            throw new RuntimeException(e);
//        }
    }

    private static class DynInstance {
        private Object instance;

        private DynInstance(Object instance) {
            this.instance = instance;
        }

        public <N> N as(Class<N> newClass) {
            try {
                N object = newClass.getConstructor().newInstance();
                copyFields(object, instance);
                return object;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        private static void copyFields(Object dest, Object src) {
            try {
                for (Field srcField : src.getClass().getDeclaredFields()) {
                    srcField.setAccessible(true);
                    if (!srcField.isSynthetic()) {
                        Field destField = dest.getClass().getDeclaredField(srcField.getName());
                        destField.setAccessible(true);
                        Object fieldValue = srcField.get(src);
                        destField.set(dest, fieldValue);
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }

        }

        static DynInstance create(Class<?> clazz, Object prototype) {
            try {
                Constructor<?> constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
                Object instance = constructor.newInstance();
                copyFields(instance, prototype);
                return new DynInstance(instance);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        Object instance() {
            return instance;
        }

        static DynInstance wrap(Object obj) {
            return new DynInstance(obj);
        }
    }


    private class DynamicClassLoader extends ClassLoader {
        public Class<?> defineClass(String name, byte[] b) {
            return defineClass(name, b, 0, b.length);
        }
    }

    private Class<?> cloneClass(Class<?> prototype, String newName) {

        ClassReader reader;
        try {
            reader = new ClassReader(prototype.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Remapper remapper = new Remapper() {
            @Override
            public String map(String typeName) {
                if (typeName.equals(prototype.getName().replace('.', '/'))) {
                    return newName.replace('.', '/');
                }
                return super.map(typeName);
            }
        };
        ClassWriter cw = new ClassWriter(0);
        ClassRemapper cr = new ClassRemapper(cw, remapper);
        reader.accept(cr, ClassReader.EXPAND_FRAMES);
        cw.visitEnd();

        DynamicClassLoader cl = new DynamicClassLoader();
        Thread.currentThread().setContextClassLoader(cl);
        return cl.defineClass(newName, cw.toByteArray());
    }

}
