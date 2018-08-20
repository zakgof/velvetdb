package com.zakgof.db.velvet.link;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.link.BiManyToManyLinkDef;
import com.zakgof.db.velvet.impl.link.BiMultiLinkDef;
import com.zakgof.db.velvet.impl.link.BiPriIndexMultiLinkDef;
import com.zakgof.db.velvet.impl.link.BiSecIndexMultiLinkDef;
import com.zakgof.db.velvet.impl.link.BiSingleLinkDef;
import com.zakgof.db.velvet.impl.link.MultiLinkDef;
import com.zakgof.db.velvet.impl.link.PriIndexMultiLinkDef;
import com.zakgof.db.velvet.impl.link.SecIndexMultiLinkDef;
import com.zakgof.db.velvet.impl.link.SingleLinkDef;

public class Links {

    public static <HK, HV, CK, CV> ISingleLinkDef<HK, HV, CK, CV> single(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return new SingleLinkDef<>(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> ISingleLinkDef<HK, HV, CK, CV> single(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        return new SingleLinkDef<>(hostEntity, childEntity, edgeKind);
    }

    public static <HK, HV, CK, CV> IMultiLinkDef<HK, HV, CK, CV> multi(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return new MultiLinkDef<>(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> IMultiLinkDef<HK, HV, CK, CV> multi(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        return new MultiLinkDef<>(hostEntity, childEntity, edgeKind);
    }

    public static <HK, HV, CK, CV> IBiSingleLinkDef<HK, HV, CK, CV> biSingle(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return BiSingleLinkDef.create(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> IBiSingleLinkDef<HK, HV, CK, CV> biSingle(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        return BiSingleLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
    }

    public static <HK, HV, CK, CV> IBiMultiLinkDef<HK, HV, CK, CV> biMulti(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return BiMultiLinkDef.create(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> IBiMultiLinkDef<HK, HV, CK, CV> biMulti(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        return BiMultiLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
    }

    public static <HK, HV, CK, CV> IBiManyToManyLinkDef<HK, HV, CK, CV> biManyToMany(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return BiManyToManyLinkDef.create(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> IBiManyToManyLinkDef<HK, HV, CK, CV> biManyToMany(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        return BiManyToManyLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
    }

    public static <HK, HV, CK, CV> void createChild(ILinkDef<HK, HV, CK, CV> linkDef, IVelvet velvet, HV a, CV b) {
        linkDef.getChildEntity().put(velvet, b);
        linkDef.connect(velvet, a, b);
    }

    public static <HK, HV, CK, CV> IMultiGetter<HK, HV, CK, CV> toMultiGetter(ILinkDef<HK, HV, CK, CV> linkDef) {
        if (linkDef instanceof IMultiLinkDef)
            return (IMultiLinkDef<HK, HV, CK, CV>) linkDef;
        if (linkDef instanceof ISingleLinkDef) {

            @SuppressWarnings("unchecked")
            ISingleGetter<HK, HV, CK, CV> single = (ISingleGetter<HK, HV, CK, CV>) linkDef;
            return new IMultiGetter<HK, HV, CK, CV>() {

                @Override
                public List<CV> get(IVelvet velvet, HV node) {
                    return Arrays.asList(single.get(velvet, node));
                }

                @Override
                public List<CK> keys(IVelvet velvet, HK key) {
                    CK singleKey = single.key(velvet, key);
                    return singleKey == null ? Collections.emptyList() : Arrays.asList(singleKey);
                }

                @Override
                public IEntityDef<HK, HV> getHostEntity() {
                    return linkDef.getHostEntity();
                }

                @Override
                public IEntityDef<CK, CV> getChildEntity() {
                    return linkDef.getChildEntity();
                }
            };
        }
        return null;
    }

    public static <HK, HV, CK, CV> ISingleGetter<HK, HV, CK, CV> toSingleGetter(final IMultiGetter<HK, HV, CK, CV> multi) {
        return new ISingleGetter<HK, HV, CK, CV>() {
            @Override
            public CV get(IVelvet velvet, HV node) {
                List<CV> links = multi.get(velvet, node);
                if (links.isEmpty())
                    return null;
                if (links.size() == 1)
                    return links.get(0);
                throw new VelvetException("Multigetter returns more than 1 entry and cannot be adapted to singlegetter " + links);
            }

            @Override
            public CK key(IVelvet velvet, HK key) {
                List<CK> linkKeys = multi.keys(velvet, key);
                if (linkKeys.isEmpty())
                    return null;
                if (linkKeys.size() == 1)
                    return linkKeys.get(0);
                throw new VelvetException("Multigetter returns more than 1 key and cannot be adapted to singlegetter " + linkKeys);
            }

            @Override
            public IEntityDef<HK, HV> getHostEntity() {
                return multi.getHostEntity();
            }

            @Override
            public IEntityDef<CK, CV> getChildEntity() {
                return multi.getChildEntity();
            }
        };
    }

    public static <HK, HV, CK extends Comparable<? super CK>, CV> IPriMultiLinkDef<HK, HV, CK, CV> pri(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return new PriIndexMultiLinkDef<>(hostEntity, childEntity);
    }

    public static <HK, HV, CK extends Comparable<? super CK>, CV> IBiPriMultiLinkDef<HK, HV, CK, CV> biPri(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return BiPriIndexMultiLinkDef.create(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV, M extends Comparable<? super M>> ISecMultiLinkDef<HK, HV, CK, CV, M> sec(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        return new SecIndexMultiLinkDef<>(hostEntity, childEntity, mclazz, metric);
    }

    public static <HK, HV, CK, CV, M extends Comparable<? super M>> IBiSecMultiLinkDef<HK, HV, CK, CV, M> biSec(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        return BiSecIndexMultiLinkDef.create(hostEntity, childEntity, mclazz, metric);
    }

}
