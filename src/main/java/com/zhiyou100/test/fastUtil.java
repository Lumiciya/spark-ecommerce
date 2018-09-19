package com.zhiyou100.test;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;


public class fastUtil {

	public static void main(String[] args) {
		IntList lists=new IntArrayList();
		lists.add(1);
		System.out.println(lists.get(0));
		ObjectList<String> list=new ObjectArrayList<String>();
		list.add("11");
		System.out.println(list.get(0));
		
		
		
	}
}
