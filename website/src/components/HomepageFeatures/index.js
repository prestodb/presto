import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Efficiency',
    description: (
      <>
        Velox democratizes runtime optimizations previously only implemented
        in individual engines.
      </>
    ),
  },
  {
    title: 'Consistency',
    description: (
      <>
        By leveraging the same execution library, compute engines can expose
        the exact same functions, data types, and semantics to their users.
      </>
    ),
  },
  {
    title: 'Reusability',
    description: (
      <>
        Features and runtime optimizations available in Velox are developed
        and maintained once, reducing engineering duplication and promoting reusability. 
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
