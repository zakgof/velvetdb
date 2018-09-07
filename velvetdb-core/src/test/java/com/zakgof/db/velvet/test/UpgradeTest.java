package com.zakgof.db.velvet.test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objenesis.ObjenesisStd;

import com.zakgof.db.velvet.VelvetFactory;
import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.test.UpgradeEntity.RenamedField;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader.Mode;
import com.zakgof.serialize.IFixer;
import com.zakgof.serialize.ZeSerializer;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_8;

public class UpgradeTest extends AVelvetTest {

    private UpgradeEntity.Original orig;

    public static void main(String[] args) {
        VelvetTestSuite.velvetProvider = () -> VelvetFactory.open("velvetdb://xodus/D://Pr//xoduslab");
        UpgradeTest test = new UpgradeTest();
        test.fixObjenesisCache();
        test.setupData();
        test.testRemoveField();
    }

    @Before
    public void setupData() {
        orig = new UpgradeEntity.Original();
        orig.strf = "HELLO";
        orig.intf = 4567;
        orig.integerf = 1234;
    }

    @Test
    public void testRemoveField() {
        UpgradeEntity.RemovedField rf = upgrade(orig, UpgradeEntity.RemovedField.class);
        Assert.assertEquals("HELLO", rf.strf);
        Assert.assertTrue(1234 == rf.integerf.intValue());
    }

    @Test
    public void testAddField() {
        UpgradeEntity.AddedField e = upgrade(orig, UpgradeEntity.AddedField.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intf);
        Assert.assertNull(e.addedArray);
    }

    @Test
    public void testRenameField() {
        UpgradeEntity.RenamedField e = upgrade(orig, UpgradeEntity.RenamedField.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(0 == e.intfWithNewName);
    }

    @Test
    public void testChangeFieldOrder() {
        UpgradeEntity.ChangeFieldOrder e = upgrade(orig, UpgradeEntity.ChangeFieldOrder.class);
        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intf);
    }

    @Test
    public void testFieldChangedType() {
        UpgradeEntity.FieldChangedType e = upgrade(orig, UpgradeEntity.FieldChangedType.class);
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

        Upgraderr<UpgradeEntity.Original, UpgradeEntity.RenamedField> upgraderr = new Upgraderr<>(UpgradeEntity.Original.class, UpgradeEntity.RenamedField.class);
        env.upgrader().fix((Class)upgraderr.clazz2, fixer);
        upgraderr.saveOriginal(orig);
        UpgradeEntity.RenamedField e = upgraderr.readUpgraded();

        Assert.assertEquals("HELLO", e.strf);
        Assert.assertTrue(1234 == e.integerf);
        Assert.assertTrue(4567 == e.intfWithNewName);
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
        @SuppressWarnings("rawtypes")
        private IKeylessEntityDef entity1;
        @SuppressWarnings("rawtypes")
        private IKeylessEntityDef entity2;
        private Object key;
        private Class<N> newClass;

        public Upgraderr(Class<O> originalClass, Class<N> newClass) {
            this.newClass = newClass;
            this.clazz1 = cloneClass(originalClass);
            env.upgrader().trackPackages(Mode.UPGRADE_ON_WRITE, "com.zakgof.db.velvet.test");
            this.entity1 = Entities.keyless(clazz1);
            this.clazz2 = cloneClass(newClass);
            this.entity2 = Entities.keyless(clazz2);
        }

        @SuppressWarnings("unchecked")
        private N readUpgraded() {
            Object result2 = env.calculate(velvet -> entity2.get(velvet, key));
            DynInstance wrappedResult2 = DynInstance.wrap(result2);
            N deflated = wrappedResult2.as(newClass);
            return deflated;
        }

        @SuppressWarnings("unchecked")
        public void saveOriginal(O originalValue) {
            DynInstance i1 = DynInstance.create(clazz1, originalValue);
            key = env.calculate(velvet -> entity1.put(velvet, i1.instance()));
        }
    }

    @Before
    public void fixObjenesisCache() {
        try {
            Field field = (ZeSerializer.class).getDeclaredField("objenesis");
            field.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, new ObjenesisStd(false));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DynInstance {
        private Object instance;

        private DynInstance(Object instance) {
            this.instance = instance;
        }

        public <N> N as(Class<N> newClass) {
            try {
                N object = newClass.newInstance();
                copyFields(object, instance);
                return object;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        private static void copyFields(Object dest, Object src) {
            try {
                for (Field srcField : src.getClass().getFields()) {
                    Field destField = dest.getClass().getDeclaredField(srcField.getName());
                    destField.setAccessible(true);
                    Object fieldValue = srcField.get(src);
                    destField.set(dest, fieldValue);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }

        }

        static DynInstance create(Class<?> clazz, Object prototype) {
            try {
                Object instance = clazz.newInstance();
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

    private Class<?> cloneClass(Class<?> clazz) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cw.visit(V1_8, ACC_PUBLIC, "com/zakgof/db/velvet/test/UpgradeEntity", null, "java/lang/Object", new String[]{});

        for(Field f : clazz.getDeclaredFields()) {
            cw.visitField(ACC_PUBLIC, f.getName(), Type.getDescriptor(f.getType()), null, null);
        }
        MethodVisitor con = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        con.visitCode();
        con.visitVarInsn(ALOAD, 0);
        con.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        con.visitInsn(RETURN);
        con.visitMaxs(1, 1);

        DynamicClassLoader cl = new DynamicClassLoader();
        return cl.defineClass("com.zakgof.db.velvet.test.UpgradeEntity", cw.toByteArray());
    }

}
