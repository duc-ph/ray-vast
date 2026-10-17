CPUS = max(n["Resources"]["CPU"] for n in ray.nodes()
           if n["Alive"] and n["Resources"].get("CPU", 0) > 0)
