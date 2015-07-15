package com.zakgof.db.velvet.old.index;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.annotation.AutoKeyed;

class IndexStorageRecord extends AutoKeyed {
  List<Integer> indices = new ArrayList<Integer>();
}