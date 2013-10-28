package com.psddev.dari.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class BootstrapPackage extends Record {

    @Indexed(unique = true)
    private String name;

    private Set<ObjectType> types;

    private boolean isInit;

    private Map<ObjectType, Set<BootstrapPackage>> typesInOtherPackages;

    private Map<ObjectType, Set<ObjectField>> missingTypes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<ObjectType> getTypes() {
        if (types == null) {
            types = new HashSet<ObjectType>();
        }
        return types;
    }

    public void setTypes(Set<ObjectType> types) {
        this.types = types;
    }

    public boolean isInit() {
        return isInit;
    }

    public void setIsInit(Boolean isInit) {
        this.isInit = isInit;
    }

    public Map<ObjectType, Set<BootstrapPackage>> getTypesInOtherPackages() {
        if (typesInOtherPackages == null) {
            typesInOtherPackages = new TreeMap<ObjectType, Set<BootstrapPackage>>();
        }
        return typesInOtherPackages;
    }

    public void setTypesInOtherPackages(Map<ObjectType, Set<BootstrapPackage>> typesInOtherPackages) {
        this.typesInOtherPackages = typesInOtherPackages;
    }

    public Map<ObjectType, Set<ObjectField>> getMissingTypes() {
        if (missingTypes == null) {
            missingTypes = new TreeMap<ObjectType, Set<ObjectField>>();
        }
        return missingTypes;
    }

    public void setMissingTypes(Map<ObjectType, Set<ObjectField>> missingTypes) {
        this.missingTypes = missingTypes;
    }

}
